import inspect
import json
import random
from datetime import datetime

from dcicutils import ff_utils, s3Utils
from magma_ff import reset_metawfr, run_metawfr, status_metawfr
from magma_ff.create_metawfr import MetaWorkflowRunFromSampleProcessing
from pipeline_utils.check_lines import check_lines, fastqs_dict

from .helpers import wfr_utils
from .helpers.confchecks import (
    ActionResult,
    CheckResult,
    action_function,
    check_function,
)
from .helpers.linecount_dicts import CNV_dict, proband_SNV_dict, trio_SNV_dict
from .helpers.wfrset_utils import lambda_limit, step_settings


FINAL_STATUS_RUNNING = "running"
FINAL_STATUS_INACTIVE = "inactive"
FINAL_STATUS_PENDING = "pending"
FINAL_STATUS_FAILED = "failed"
FINAL_STATUS_TO_RUN = [
    FINAL_STATUS_RUNNING,
    FINAL_STATUS_INACTIVE,
    FINAL_STATUS_PENDING,
]
FINAL_STATUS_TO_CHECK = [FINAL_STATUS_RUNNING]
FINAL_STATUS_TO_RESET = [FINAL_STATUS_FAILED]
FINAL_STATUS_TO_KILL = [
    FINAL_STATUS_RUNNING,
    FINAL_STATUS_INACTIVE,
    FINAL_STATUS_PENDING,
    FINAL_STATUS_FAILED,
]
SPOT_FAILURE_DESCRIPTIONS = ["EC2 unintended termination", "EC2 Idle error"]


# TODO: Figure out line_count check with Michele --> should really be a QualityMetric
# produced by MWFR


def initialize_check(connection):
    """Create a CheckResult with default attributes."""
    function_called_by = inspect.stack()[1].function
    check = CheckResult(connection, function_called_by)
    check.brief_output = []
    check.full_output = {}
    check.status = "PASS"
    check.allow_action = True
    return check


def initialize_action(connection, kwargs):
    """Create an ActionResult with default attributes."""
    function_called_by = inspect.stack()[1].function
    action = ActionResult(connection, function_called_by)
    action.status = "DONE"
    action.output = {}
    check_result = action.get_associated_check_result(kwargs).get("full_output", {})
    return action, check_result


def format_kwarg_list(kwarg_input):
    """Ensure kwarg is a list of strings."""
    if isinstance(kwarg_input, str):
        result = []
        no_space_input = kwarg_input.replace(" ", ",")
        split_input = no_space_input.split(",")
        for input_item in split_input:
            stripped_item = input_item.strip()
            if stripped_item:
                result.append(stripped_item)
    elif isinstance(kwarg_input, list):
        result = kwarg_input
    elif kwarg_input is None:
        result = []
    else:
        raise Exception("Couldn't format kwarg input: %s" % kwarg_input)
    return result


def validate_items_existence(item_identifiers, connection):
    """Get raw view of items from database and keep track of which
    identifiers could not be retrieved.
    """
    found = []
    not_found = []
    if isinstance(item_identifiers, str):
        item_identifiers = [item_identifiers]
    for item_identifier in item_identifiers:
        try:
            item = ff_utils.get_metadata(
                item_identifier,
                key=connection.ff_keys,
                add_on="frame=raw&datastore=database",
            )
            found.append(item)
        except Exception:
            not_found.append(item_identifier)
    return found, not_found


def add_to_dict_as_list(dictionary, key, value):
    """Add key, value pair to dictionary, with values for key stored in
    list.
    """
    existing_item_value = dictionary.get(key)
    if existing_item_value:
        existing_item_value.append(value)
    else:
        dictionary[key] = [value]


def get_and_add_field(items, field, list_to_append):
    """Add field from items to given list."""
    for item in items:
        item_uuid = item.get(field)
        if item_uuid is not None:
            list_to_append.append(item_uuid)


def get_and_add_uuids(items, list_to_append):
    """Add UUIDs from items to given list."""
    get_and_add_field(items, "uuid", list_to_append)


def make_embed_request(ids, fields, connection):
    """POST to /embed API to get desired fields for all given
    identifiers.
    """
    result = []
    if isinstance(ids, str):
        ids = [ids]
    if isinstance(fields, str):
        fields = [fields]
    id_chunks = chunk_ids(ids)
    for id_chunk in id_chunks:
        post_body = {"ids": ids, "fields": fields}
        endpoint = connection.ff_server + "embed"
        embed_response = ff_utils.authorized_request(
            endpoint, verb="POST", auth=connection.ff_keys, data=json.dumps(post_body)
        ).json()
        result += embed_response
    if len(result) == 1:
        result = result[0]
    return result


def chunk_ids(ids):
    """Split list into list of lists of maximum chunk size length.

    Embed API currently accepts max 5 identifiers, so chunk size is 5.
    """
    result = []
    chunk_size = 5
    for idx in range(0, len(ids), chunk_size):
        result.append(ids[idx: idx + chunk_size])
    return result


@check_function(file_type="File", start_date=None)
def md5runCGAP_status(connection, file_type="", start_date=None, **kwargs):
    """
    Searches for files that are uploaded to s3, but not went through md5 run
    Check assumptions:
        - all files that have a status uploaded run through md5runCGAP
        - all files status uploading/upload failed and NO s3 file are skipped

    If the status is changed manually, it might fail to show up in this checkself

    kwargs:
        file_type -- limit search to a file type, i.e. FileFastq (default=File)
        start_date -- limit search to files generated since date  YYYY-MM-DD
        run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    check = initialize_check(connection)
    check.action = "md5runCGAP_start"

    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = "PASS"
        check.brief_output = ["Waiting for indexing queue to clear"]
        check.summary = "Waiting for indexing queue to clear"
        check.full_output = {}
        check.allow_action = False
        return check
    start = datetime.utcnow()
    my_auth = connection.ff_keys
    query = "/search/?status=uploading&status=upload failed"
    query += "&type=" + file_type
    if start_date is not None:
        query += "&date_created.from=" + start_date
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = "All Good!"
        check.allow_action = False
        return check
    # if there are files, make sure they are not on s3
    no_s3_file = []
    running = []
    missing_md5 = []
    not_switched_status = []
    problems = []  # multiple failed runs
    my_s3_util = s3Utils(env=env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket
    for a_file in res:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            check.brief_output.append("did not complete checking all")
            break
        # find bucket
        if "FileProcessed" in a_file["@type"]:
            my_bucket = out_bucket
        # elif 'FileVistrack' in a_file['@type']:
        #         my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
            my_bucket = raw_bucket
        # check if file is in s3
        file_id = a_file["accession"]
        head_info = my_s3_util.does_key_exist(a_file["upload_key"], my_bucket)
        if not head_info:
            no_s3_file.append(file_id)
            continue
        md5_report = wfr_utils.get_wfr_out(a_file, "md5", key=my_auth, md_qc=True)
        if md5_report["status"] == "running":
            running.append(file_id)
        elif md5_report["status"].startswith("no complete run, too many"):
            problems.append(file_id)
        # most probably the trigger did not work, and we run it manually
        elif md5_report["status"] != "complete":
            missing_md5.append(file_id)
        # there is a successful run, but status is not switched, happens when a file is reuploaded.
        elif md5_report["status"] == "complete":
            not_switched_status.append(file_id)
    if no_s3_file:
        check.summary = "Some files are pending upload"
        msg = (
            str(len(no_s3_file)) + "(uploading/upload failed) files waiting for upload"
        )
        check.brief_output.append(msg)
        check.full_output["files_pending_upload"] = no_s3_file
    if running:
        check.summary = "Some files are running md5runCGAP"
        msg = str(len(running)) + " files are still running md5runCGAP."
        check.brief_output.append(msg)
        check.full_output["files_running_md5"] = running
    if problems:
        check.summary = "Some files have problems"
        msg = str(len(problems)) + " file(s) have problems."
        check.brief_output.append(msg)
        check.full_output["problems"] = problems
        check.status = "WARN"
    if missing_md5:
        check.summary = "Some files are missing md5 runs"
        msg = str(len(missing_md5)) + " file(s) lack a successful md5 run"
        check.brief_output.append(msg)
        check.full_output["files_without_md5run"] = missing_md5
        check.status = "WARN"
    if not_switched_status:
        check.summary += " Some files are have wrong status with a successful run"
        msg = (
            str(len(not_switched_status))
            + " file(s) are have wrong status with a successful run"
        )
        check.brief_output.append(msg)
        check.full_output["files_with_run_and_wrong_status"] = not_switched_status
        check.status = "WARN"
    if not check.brief_output:
        check.brief_output = [
            "All Good!",
        ]
        check.allow_action = False
    check.summary = check.summary.strip()
    return check


@action_function(start_missing=True, start_not_switched=True)
def md5runCGAP_start(connection, **kwargs):
    """
    Start md5 runs by sending compiled input_json to run_workflow endpoint
    """
    start = datetime.utcnow()
    action = ActionResult(connection, "md5runCGAP_start")
    action_logs = {"runs_started": [], "runs_failed": []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    sfn = "tibanna_zebra_" + env.replace("fourfront-", "")
    md5runCGAP_check_result = action.get_associated_check_result(kwargs).get(
        "full_output", {}
    )
    action_logs["check_output"] = md5runCGAP_check_result
    targets = []
    if kwargs.get("start_missing"):
        targets.extend(md5runCGAP_check_result.get("files_without_md5run", []))
    if kwargs.get("start_not_switched"):
        targets.extend(
            md5runCGAP_check_result.get("files_with_run_and_wrong_status", [])
        )
    action_logs["targets"] = targets
    for a_target in targets:
        print("processing target %s" % a_target)
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        print("getting metadata for target...")
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        print("getting attribution for target...")
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {
            "input_file": a_file["uuid"],
            "additional_file_parameters": {"input_file": {"mount": True}},
        }
        print("input template for target: %s" % str(inp_f))
        wfr_setup = step_settings("md5", "no_organism", attributions)
        print("wfr_setup for target: %s" % str(wfr_setup))
        url = wfr_utils.run_missing_wfr(
            wfr_setup,
            inp_f,
            a_file["accession"],
            connection.ff_keys,
            connection.ff_env,
            sfn,
        )
        # aws run url
        if url.startswith("http"):
            action_logs["runs_started"].append(url)
        else:
            action_logs["runs_failed"].append([a_target, url])
    action.output = action_logs
    return action


# Check and action below should probably be wrapped up in the MWFs as QualityMetrics
# so that the results can be evaluated. Otherwise, no automated action can be taken
# on the output.
@check_function()
def metawfrs_to_check_linecount(connection, **kwargs):
    """
    Find 'completed' metaworkflowruns
    Run a line_count_test qc check
    """
    check = initialize_check(connection)
    check.action = "line_count_test"
    check.description = "Find MetaWorkflowRuns that need linecount QC check."

    meta_workflow_run_uuids = []
    meta_workflow_run_titles = []
    my_auth = connection.ff_keys
    query = (  # Overall QCs not empty but missing linecount
        "search/?type=MetaWorkflowRun&final_status=completed&field=uuid&field=title"
        "&overall_qcs.name!=linecount_test"
    )
    search_result = ff_utils.search_metadata(query, key=my_auth)
    get_and_add_uuids(search_result, meta_workflow_run_uuids)
    get_and_add_field(search_result, "title", meta_workflow_run_titles)
    query = (  # No overall QCs
        "search/?type=MetaWorkflowRun&final_status=completed&field=uuid&field=title"
        "&overall_qcs=No+value"
    )
    search_result = ff_utils.search_metadata(query, key=my_auth)
    get_and_add_uuids(search_result, meta_workflow_run_uuids)
    get_and_add_field(search_result, "title", meta_workflow_run_titles)
    msg = "%s MetaWorkflowRun(s) may require line count checks." % len(
        meta_workflow_run_uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.status = "WARN"
    check.full_output["metawfrs_to_run"] = {
        "titles": meta_workflow_run_titles,
        "uuids": meta_workflow_run_uuids,
    }
    if not meta_workflow_run_uuids:
        check.allow_action = False
    return check


@action_function()
def line_count_test(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, "line_count_test")
    action_logs = {
        "metawfrs_that_passed_linecount_test": [],
        "metawfrs_that_failed_linecount_test": [],
    }
    my_auth = connection.ff_keys
    check_result = action.get_associated_check_result(kwargs).get("full_output", {})
    action_logs["check_output"] = check_result
    action_logs["error"] = []
    metawfr_uuids = check_result.get("metawfrs_to_run", {}).get("uuids", [])
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            metawfr_meta = ff_utils.get_metadata(
                metawfr_uuid, add_on="frame=raw", key=my_auth
            )
            # we have a few different dictionaries of steps to check output from in linecount_dicts.py
            # the proband-only and family workflows have the same steps, so we assign the proband_SNV_dict
            if (
                "Proband-only" in metawfr_meta["title"]
                or "Family" in metawfr_meta["title"]
            ):
                steps_dict = proband_SNV_dict
            # trio has novoCaller, so it has a separate dictionary of steps
            elif "Trio" in metawfr_meta["title"]:
                steps_dict = trio_SNV_dict
            # cnv/sv is a completely different pipeline, so has a many different steps
            elif "CNV" in metawfr_meta["title"]:
                steps_dict = CNV_dict
            # if this is run on something other than those expected MWFRs, we want an error.
            else:
                e = "Unexpected MWF Title: " + metawfr_meta["title"]
                action_logs["error"].append(str(e))
                continue
            # this calls check_lines from cgap-pipeline pipeline_utils check_lines.py (might get moved to generic repo in the future)
            # will return TRUE or FALSE if all pipeline steps are fine, or if there are any that do not match linecount with their partners, respectively
            linecount_result = check_lines(
                metawfr_uuid, my_auth, steps=steps_dict, fastqs=fastqs_dict
            )
            # want an empty dictionary if no overall_qcs, or a dictionary of tests and results if there are items in the overall_qcs list
            overall_qcs_dict = {
                qc["name"]: qc["value"] for qc in metawfr_meta.get("overall_qcs", [])
            }
            overall_qcs_dict["linecount_test"] = "PASS" if linecount_result else "FAIL"
            # turn the dictionary back into a list of dictionaries that is properly structured (e.g., overall_qcs: [{"name": "linecount_test", "value": "PASS"}, {...}, {...}])
            updated_overall_qcs = [
                {"name": k, "value": v} for k, v in overall_qcs_dict.items()
            ]
            try:
                ff_utils.patch_metadata(
                    {"overall_qcs": updated_overall_qcs}, metawfr_uuid, key=my_auth
                )
                if linecount_result:
                    action_logs["metawfrs_that_passed_linecount_test"].append(
                        metawfr_uuid
                    )
                else:
                    action_logs["metawfrs_that_failed_linecount_test"].append(
                        metawfr_uuid
                    )
            except Exception as e:
                action_logs["error"].append(str(e))
                continue
        except Exception as e:
            action_logs["error"].append(str(e))
            continue
    action.output = action_logs
    return action


@check_function()
def metawfrs_to_run(connection, **kwargs):
    """Find MetaWorkflowRuns that may have WorkflowRuns to kick."""
    check = initialize_check(connection)
    check.action = "run_metawfrs"
    check.description = "Find MetaWorkflowRuns that have WorkflowRuns to kick."

    meta_workflow_run_uuids = []
    meta_workflow_run_titles = []
    query = "/search/?type=MetaWorkflowRun&field=uuid&field=title" + "".join(
        ["&final_status=" + st for st in FINAL_STATUS_TO_RUN]
    )
    search_response = ff_utils.search_metadata(query, key=connection.ff_keys)
    get_and_add_uuids(search_response, meta_workflow_run_uuids)
    get_and_add_field(search_response, "title", meta_workflow_run_titles)
    msg = "%s MetaWorkflowRun(s) may have WorkflowRuns to kick" % len(
        meta_workflow_run_uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_run_uuids,
        "titles": meta_workflow_run_titles,
    }
    if not meta_workflow_run_uuids:
        check.allow_action = False
    return check


@action_function()
def run_metawfrs(connection, **kwargs):
    """Kick WorkflowRuns on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Start WorkflowRuns for MetaWorkflowRuns"

    success = []
    error = {}
    env = connection.ff_env
    sfn = "tibanna_zebra_" + env.replace("fourfront-", "")
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            run_metawfr.run_metawfr(
                meta_workflow_run_uuid,
                connection.ff_keys,
                sfn=sfn,
                env=env,
                valid_status=FINAL_STATUS_TO_RUN,
            )
            success.append(meta_workflow_run_uuid)
        except Exception as e:
            error[meta_workflow_run_uuid] = str(e)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function()
def metawfrs_to_checkstatus(connection, **kwargs):
    """Find MetaWorkflowRuns that may require a status check."""
    check = initialize_check(connection)
    check.action = "checkstatus_metawfrs"
    check.description = "Find MetaWorkflowRuns with WorkflowRuns to status check."

    meta_workflow_run_uuids = []
    meta_workflow_run_titles = []
    query = "/search/?type=MetaWorkflowRun&field=uuid&field=title" + "".join(
        ["&final_status=" + st for st in FINAL_STATUS_TO_CHECK]
    )
    search_response = ff_utils.search_metadata(query, key=connection.ff_keys)
    get_and_add_uuids(search_response, meta_workflow_run_uuids)
    get_and_add_field(search_response, "title", meta_workflow_run_titles)
    msg = "%s MetaWorkflowRun(s) may have WorkflowRuns to status check" % len(
        meta_workflow_run_uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_run_uuids,
        "titles": meta_workflow_run_titles,
    }
    if not meta_workflow_run_uuids:
        check.allow_action = False
    return check


@action_function()
def checkstatus_metawfrs(connection, **kwargs):
    """Check WorkflowRuns' status on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Update WorkflowRuns' status on MetaWorkflowRuns"

    success = []
    error = {}
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            status_metawfr.status_metawfr(
                meta_workflow_run_uuid,
                connection.ff_keys,
                env=connection.ff_env,
                valid_status=FINAL_STATUS_TO_CHECK,
            )
            success.append(meta_workflow_run_uuid)
        except Exception as e:
            error[meta_workflow_run_uuid] = str(e)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function()
def spot_failed_metawfrs(connection, **kwargs):
    """Find MetaWorkflowRuns with failed WorkflowRuns from spot
    interruptions.
    """
    check = initialize_check(connection)
    check.action = "reset_spot_failed_metawfrs"
    check.description = (
        "Find MetaWorkflowRuns with failed WorkflowRuns to reset spot failures"
    )

    meta_workflow_run_uuids = []
    meta_workflow_run_titles = []
    query = "/search/?type=MetaWorkflowRun&field=uuid&field=title" + "".join(
        ["&final_status=" + st for st in FINAL_STATUS_TO_RESET]
    )
    search_response = ff_utils.search_metadata(query, key=connection.ff_keys)
    get_and_add_uuids(search_response, meta_workflow_run_uuids)
    get_and_add_field(search_response, "title", meta_workflow_run_titles)
    msg = "%s MetaWorkflowRun(s) may have spot-failed WorkflowRuns to reset" % len(
        meta_workflow_run_uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_run_uuids,
        "titles": meta_workflow_run_titles,
    }
    if not meta_workflow_run_uuids:
        check.allow_action = False
    return check


@action_function()
def reset_spot_failed_metawfrs(connection, **kwargs):
    """Reset spot-failed WorkflowRuns on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Reset spot-failed WorkflowRuns on MetaWorkflowRuns"

    success = {}
    error = {}
    s3_utils = s3Utils(env=connection.ff_env)
    log_bucket = s3_utils.tibanna_output_bucket
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            shards_to_reset = []
            meta_workflow_run = ff_utils.get_metadata(
                meta_workflow_run_uuid,
                add_on="frame=raw&datastore=database",
                key=connection.ff_keys,
            )
            workflow_runs = meta_workflow_run.get("workflow_runs", [])
            for workflow_run in workflow_runs:
                workflow_run_status = workflow_run.get("status")
                workflow_run_jobid = workflow_run.get("jobid")
                workflow_run_shard = workflow_run.get("shard")
                workflow_run_name = workflow_run.get("name")
                if workflow_run_status == "failed":
                    query = (
                        "/search/?type=WorkflowRunAwsem&awsem_job_id=%s"
                        % workflow_run_jobid
                    )
                    search_response = ff_utils.search_metadata(
                        query, key=connection.ff_keys
                    )
                    if len(search_response) == 1:
                        workflow_run_awsem = search_response[0]
                    elif len(search_response) > 1:
                        msg = (
                            "Multiple WorkflowRunAwsem found for job ID: %s"
                            % workflow_run_jobid
                        )
                        raise Exception(msg)
                    else:
                        msg = (
                            "No WorkflowRunAwsem found for job ID: %s"
                            % workflow_run_jobid
                        )
                        raise Exception(msg)
                    workflow_run_awsem_description = workflow_run_awsem.get(
                        "description"
                    )
                    spot_failure_descriptions = [
                        spot_description in workflow_run_awsem_description
                        for spot_description in SPOT_FAILURE_DESCRIPTIONS
                    ]
                    log_bucket_spot_failure = s3_utils.does_key_exist(
                        key=workflow_run_jobid + ".spot_failure",
                        bucket=log_bucket,
                        print_error=False,
                    )
                    if log_bucket_spot_failure or any(spot_failure_descriptions):
                        shard_name = workflow_run_name + ":" + str(workflow_run_shard)
                        shards_to_reset.append(shard_name)
            if shards_to_reset:
                reset_metawfr.reset_shards(
                    meta_workflow_run_uuid,
                    shards_to_reset,
                    connection.ff_keys,
                    valid_status=FINAL_STATUS_TO_RESET,
                )
                success[meta_workflow_run_uuid] = {"shards_reset": shards_to_reset}
        except Exception as e:
            error[meta_workflow_run_uuid] = str(e)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function(meta_workflow_runs=None)
def failed_metawfrs(connection, meta_workflow_runs=None, **kwargs):
    """Find failed MetaWorkflowRuns and reset failed WorkflowRuns."""
    check = initialize_check(connection)
    check.action = "reset_failed_metawfrs"
    check.description = "Find failed MetaWorkflowRuns to reset all failed WorkflowRuns."

    meta_workflow_run_uuids = []
    meta_workflow_run_titles = []
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        get_and_add_uuids(found, meta_workflow_run_uuids)
        get_and_add_field(found, "title", meta_workflow_run_titles)
        meta_workflow_runs_not_found += not_found
    else:
        query = "/search/?type=MetaWorkflowRun" + "".join(
            ["&final_status=" + st for st in FINAL_STATUS_TO_RESET]
        )
        search_response = ff_utils.search_metadata(query, key=connection.ff_keys)
        get_and_add_uuids(search_response, meta_workflow_run_uuids)
        get_and_add_field(search_response, "title", meta_workflow_run_titles)
    msg = "%s MetaWorkflowRun(s) have failed WorkflowRuns to reset" % len(
        meta_workflow_run_uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_run_uuids,
        "titles": meta_workflow_run_titles,
    }
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRun identifiers could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
        check.status = "WARN"
    if not meta_workflow_run_uuids:
        check.allow_action = False
    return check


@action_function()
def reset_failed_metawfrs(connection, **kwargs):
    """Reset all failed WorkflowRuns on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Reset all failed WorkflowRuns on MetaWorkflowRuns"

    success = []
    error = {}
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            reset_metawfr.reset_failed(
                meta_workflow_run_uuid,
                connection.ff_keys,
                valid_status=FINAL_STATUS_TO_RESET,
            )
            success.append(meta_workflow_run_uuid)
        except Exception as e:
            error[meta_workflow_run_uuid] = str(e)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function(start_date=None, file_accessions=None)
def ingest_vcf_status(connection, start_date=None, file_accessions=None, **kwargs):
    """Search for full annotated VCF files that need to be ingested.

    kwargs:
        start_date -- limit search to files generated since a date formatted YYYY-MM-DD
        file_accession -- run check with given files instead of the default query
                          expects comma/space separated accessions
    """
    check = initialize_check(connection)
    check.action = "ingest_vcf_start"
    check.description = "Find VCFs to ingest"

    vcfs_to_ingest_uuids = []
    vcfs_to_ingest_accessions = []
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        msg = "Waiting for indexing queue to clear"
        check.brief_output.append(msg)
        check.summary = msg
        check.allow_action = False
        return check
    old_style_query = (
        "/search/?file_type=full+annotated+VCF&type=FileProcessed"
        "&file_ingestion_status=No value&file_ingestion_status=N/A"
        "&status!=uploading&status!=to be uploaded by workflow&status!=upload failed"
    )
    new_style_query = (
        "/search/?vcf_to_be_ingested=true&type=FileProcessed"
        "&file_ingestion_status=No value&file_ingestion_status=N/A"
        "&status!=uploading&status!=to be uploaded by workflow&status!=upload failed"
    )
    queries = [old_style_query, new_style_query]
    if start_date:
        for idx, query in enumerate(queries):
            query += "&date_created.from=" + start_date
            queries[idx] = query
    if file_accessions:
        file_accessions = format_kwarg_list(file_accessions)
        for idx, query in enumerate(queries):
            for an_acc in file_accessions:
                query += "&accession={}".format(an_acc)
            queries[idx] = query
    for query in queries:
        search_results = ff_utils.search_metadata(query, key=connection.ff_keys)
        get_and_add_uuids(search_results, vcfs_to_ingest_uuids)
        get_and_add_field(search_results, "accession", vcfs_to_ingest_accessions)
    if not vcfs_to_ingest_uuids:
        check.summary = "All Good!"
        check.allow_action = False
        return check
    msg = "{} file(s) will be added to the ingestion queue".format(
        str(len(vcfs_to_ingest_uuids))
    )
    check.brief_output.append(msg)
    check.summary = msg
    check.full_output = {
        "files": vcfs_to_ingest_uuids,
        "accessions": vcfs_to_ingest_accessions,
    }
    return check


@action_function()
def ingest_vcf_start(connection, **kwargs):
    """POST VCF UUIDs to ingestion endpoint."""
    action, check_result = initialize_action(connection, kwargs)

    my_auth = connection.ff_keys
    targets = check_result["files"]
    post_body = {"uuids": targets}
    try:
        ff_utils.post_metadata(post_body, "/queue_ingestion", key=my_auth)
        action.output["queued for ingestion"] = targets
    except Exception as e:
        action.output["error"] = str(e)
    return action


@check_function(file_accessions=None)
def check_vcf_ingestion_errors(connection, file_accessions=None, **kwargs):
    """
    Check for finding full annotated VCFs that have failed ingestion, so that they
    can be reset and the ingestion rerun if needed.
    """
    check = initialize_check(connection)
    check.action = "reset_vcf_ingestion_errors"
    check.description = (
        "Find VCFs that have failed ingestion to clear metadata for reingestion"
    )

    files_with_ingestion_errors = {}
    accessions = format_kwarg_list(file_accessions)
    ingestion_error_search = "search/?type=FileProcessed&file_ingestion_status=Error"
    if accessions:
        ingestion_error_search += "&accession="
        ingestion_error_search += "&accession=".join(accessions)
    ingestion_error_search += "&field=@id&field=file_ingestion_error"
    search_response = ff_utils.search_metadata(
        ingestion_error_search, key=connection.ff_keys
    )
    for result in search_response:
        file_atid = result.get("@id")
        first_error = None
        ingestion_errors = result.get("file_ingestion_error", [])
        if ingestion_errors:
            # usually there are 100 errors, but just report first error here
            first_error = ingestion_errors[0].get("body")
        files_with_ingestion_errors[file_atid] = first_error
    msg = "%s File(s) found with ingestion errors" % len(search_response)
    check.brief_output.append(msg)
    check.summary = msg
    check.full_output = files_with_ingestion_errors
    if files_with_ingestion_errors:
        check.status = "WARN"
    else:
        check.allow_action = False
    return check


@action_function()
def reset_vcf_ingestion_errors(connection, **kwargs):
    """Reset VCF metadata for reingestion."""
    action, check_result = initialize_action(connection, kwargs)

    success = []
    error = []
    for vcf_atid in check_result:
        patch = {"file_ingestion_status": "N/A"}
        try:
            ff_utils.patch_metadata(
                patch,
                vcf_atid + "?delete_fields=file_ingestion_error",
                key=connection.ff_keys,
            )
            success.append(vcf_atid)
        except Exception as e:
            error[vcf_atid] = str(e)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function()
def find_meta_workflow_runs_requiring_output_linktos(connection, **kwargs):
    """Find completed MetaWorkflowRuns to PATCH output files to desired
    locations.
    """
    check = initialize_check(connection)
    check.description = "Find completed MetaWorkflowRuns to PATCH their output files."
    check.action = "link_meta_workflow_run_output_files"

    meta_workflow_runs = []
    query = (
        "search/?type=MetaWorkflowRun&final_status=completed&field=uuid"
        "&output_files_linked_status=No+value"
    )
    search_response = ff_utils.search_metadata(query, key=connection.ff_keys)
    get_and_add_uuids(search_response, meta_workflow_runs)
    msg = "%s MetaworkflowRun(s) found to PATCH output files" % len(meta_workflow_runs)
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = meta_workflow_runs
    if not meta_workflow_runs:
        check.allow_action = False
    return check


@action_function()
def link_meta_workflow_run_output_files(connection, **kwargs):
    """PATCH MetaWorkflowRuns' designated output files to desired
    locations.
    """
    action, check_result = initialize_action(connection, kwargs)
    action.description = "PATCH MetaWorkflowRuns' output files"

    success = []
    error = []
    meta_workflow_run_uuids = check_result.get("meta_workflow_runs", [])
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        successful_links = create_output_file_links(meta_workflow_run_uuid, connection)
        if successful_links:
            success.append(meta_workflow_run_uuid)
        else:
            error.append(meta_workflow_run_uuid)
    action.output["success"] = success
    action.output["error"] = error
    return action


def create_output_file_links(meta_workflow_run_uuid, connection):
    """For given MetaWorkflowRun, collect output files requiring PATCH,
    attempt PATCHes, and update MetaWorkflowRun metadata per results.
    """
    result = True
    output_files_to_link = {}
    file_linkto_field = "linkto_location"
    embed_fields = [
        "input_samples",
        "associated_sample_processing",
        "workflow_runs.shard",
        "workflow_runs.output.file.uuid",
        "workflow_runs.output.file." + file_linkto_field,
    ]
    meta_workflow_run = make_embed_request(
        meta_workflow_run_uuid, embed_fields, connection
    )
    workflow_runs = meta_workflow_run.get("workflow_runs", [])
    for workflow_run in workflow_runs:
        output_files = workflow_run.get("output", [])
        shard = workflow_run.get("shard")
        for output in output_files:
            output_file = output.get("file", {})
            output_file_linkto_field = output_file.get(file_linkto_field, [])
            if not output_file_linkto_field:
                continue
            output_file_uuid = output_file.get("uuid")
            sample_idx = int(shard.split(":")[0])
            file_fields = {"file_uuid": output_file_uuid, "sample_idx": sample_idx}
            for linkto_location in output_file_linkto_field:
                add_to_dict_as_list(output_files_to_link, linkto_location, file_fields)
    if output_files_to_link:
        input_samples = meta_workflow_run.get("input_samples")
        sample_processing = meta_workflow_run.get("associated_sample_processing")
        link_errors = create_file_linktos(
            output_files_to_link,
            input_samples,
            sample_processing,
            connection=connection,
        )
        update_meta_workflow_run_files_linked(
            meta_workflow_run_uuid, errors=link_errors, connection=connection
        )
        if link_errors:
            result = False
    else:
        update_meta_workflow_run_files_linked(
            meta_workflow_run_uuid, connection=connection
        )
    return result


def create_file_linktos(
    output_files_to_link, input_sample_uuids, sample_processing_uuid, connection=None
):
    """Perform PATCHes for given output files.

    NOTE: File.linkto_location values are handled here; any new values
    require updating function.
    """
    linkto_errors = {}
    to_patch = {}
    for linkto_location, files_to_link in output_files_to_link.items():
        if linkto_location == "Sample":
            for file_to_link in files_to_link:
                file_uuid = file_to_link.get("file_uuid")
                if input_sample_uuids is None:
                    error_msg = "No input_samples available on MetaWorkflowRun"
                    add_to_dict_as_list(linkto_errors, file_uuid, error_msg)
                    continue
                sample_idx = file_to_link.get("sample_idx")
                sample_uuid = input_sample_uuids[sample_idx]
                add_to_dict_as_list(to_patch, sample_uuid, file_uuid)
        elif linkto_location == "SampleProcessing":
            for file_to_link in files_to_link:
                file_uuid = file_to_link.get("file_uuid")
                if sample_processing_uuid is None:
                    error_msg = (
                        "No associated_sample_processing available on MetaWorkflowRun"
                    )
                    add_to_dict_as_list(linkto_errors, file_uuid, error_msg)
                    continue
                add_to_dict_as_list(to_patch, sample_processing_uuid, file_uuid)
        else:
            for file_to_link in files_to_link:
                file_uuid = file_to_link.get("file_uuid")
                error_msg = "File linkto_location was unexpected: %s" % linkto_location
                add_to_dict_as_list(linkto_errors, file_uuid, error_msg)
    for item_to_patch, files_to_patch in to_patch.items():
        need_to_patch = False
        item = ff_utils.get_metadata(
            item_to_patch, key=connection.ff_keys, add_on="frame=raw&datastore=database"
        )
        item_processed_files = item.get("processed_files", [])
        processed_files_to_patch = []
        processed_files_to_patch += item_processed_files
        for file_to_patch in files_to_patch:
            if file_to_patch not in processed_files_to_patch:
                processed_files_to_patch.append(file_to_patch)
        if len(processed_files_to_patch) > len(item_processed_files):
            need_to_patch = True
        if need_to_patch:
            patch_body = {"processed_files": processed_files_to_patch}
            try:
                ff_utils.patch_metadata(
                    patch_body, obj_id=item_to_patch, key=connection.ff_keys
                )
            except Exception as error_msg:
                for file_uuid in files_to_patch:
                    add_to_dict_as_list(linkto_errors, file_uuid, str(error_msg))
    return linkto_errors


def update_meta_workflow_run_files_linked(
    meta_workflow_run_uuid, errors=None, connection=None
):
    """PATCH MetaWorkflowRun metadata related to status of output file
    PATCHes.
    """
    if not errors:
        patch_body = {"output_files_linked_status": "success"}
        ff_utils.patch_metadata(
            patch_body, meta_workflow_run_uuid, key=connection.ff_keys
        )
    else:
        output_files_linked_errors = []
        for file_uuid, linkto_errors in errors.items():
            output_files_linked_errors.append(
                {"output_file": file_uuid, "errors": linkto_errors}
            )
        patch_body = {
            "output_files_linked_status": "error",
            "output_files_linked_errors": output_files_linked_errors,
        }
        ff_utils.patch_metadata(
            patch_body, meta_workflow_run_uuid, key=connection.ff_keys
        )


@check_function(meta_workflow_runs=None)
def find_meta_workflow_runs_with_linkto_errors(
    connection, meta_workflow_runs=None, **kwargs
):
    """Find MetaWorkflowRuns with output file linkTo creation errors."""
    check = initialize_check(connection)
    check.action = "link_meta_workflow_run_output_files_after_error"
    check.description = "Find MetaWorkflowRuns with errors creating output file linkTos"

    meta_workflow_runs_found = []
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        for meta_workflow_run in found:
            meta_workflow_run_uuid = meta_workflow_run.get("uuid")
            linked_status = meta_workflow_run.get("output_files_linked_status")
            if linked_status == "error":
                meta_workflow_runs_found.append(meta_workflow_run_uuid)
        meta_workflow_runs_not_found += not_found
    else:
        query = (
            "search/?type=MetaWorkflowRun&field=uuid&output_files_linked_status=error"
        )
        search = ff_utils.search_metadata(query, key=connection.ff_keys)
        get_and_add_uuids(search, meta_workflow_runs_found)
    msg = "%s MetaWorkflowRun(s) found with errors for output file links" % len(
        meta_workflow_runs_found
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = meta_workflow_runs_found
    if meta_workflow_runs_found:
        check.status = "WARN"
    else:
        check.allow_action = False
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRun(s) could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    return check


@action_function()
def link_meta_workflow_run_output_files_after_error(connection, **kwargs):
    """Attempt to PATCH output files to desired locations on a
    MetaWorkflowRun with prior errors.
    """
    action, check_result = initialize_action(connection, kwargs)
    action.description = "PATCH MetaWorkflowRuns' output files after prior error"

    success = []
    error = []
    meta_workflow_run_uuids = check_result.get("meta_workflow_runs", [])
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        successful_links = create_output_file_links(meta_workflow_run_uuid, connection)
        if successful_links:
            ff_utils.delete_field(
                meta_workflow_run_uuid,
                "output_files_linked_errors",
                key=connection.ff_keys,
            )
            success.append(meta_workflow_run_uuid)
        else:
            error.append(meta_workflow_run_uuid)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function(meta_workflow_run_uuids=None, meta_workflow_uuids=None)
def find_meta_workflow_runs_to_kill(
    connection, meta_workflow_run_uuids=None, meta_workflow_uuids=None, **kwargs
):
    """Find MetaWorkflowRuns to stop (won't be picked up by other
    MetaWorkflowRun checks/actions).
    """
    check = initialize_check(connection)
    check.description = "Find MetaWorkflowRuns to stop further checks/actions"
    check.action = "kill_meta_workflow_runs"

    meta_workflow_runs_to_kill = []
    meta_workflow_runs_not_found = []
    if meta_workflow_run_uuids is not None:
        meta_workflow_run_uuids = format_kwarg_list(meta_workflow_run_uuids)
        found, not_found = validate_items_existence(meta_workflow_run_uuids, connection)
        get_and_add_uuids(found, meta_workflow_runs_to_kill)
        meta_workflow_runs_not_found += not_found
    if meta_workflow_uuids is not None:
        meta_workflow_uuids = format_kwarg_list(meta_workflow_uuids)
        for meta_workflow_uuid in meta_workflow_uuids:
            search_query = (
                "search/?type=MetaWorkflowRun&field=uuid&meta_workflow.uuid="
                + meta_workflow_uuid
                + "".join(
                    ["&final_status=" + status for status in FINAL_STATUS_TO_KILL]
                )
            )
            search_results = ff_utils.search_metadata(
                search_query, key=connection.ff_keys
            )
            get_and_add_uuids(search_results, meta_workflow_runs_to_kill)
    if meta_workflow_uuids is None and meta_workflow_run_uuids is None:
        query = (
            "search/?type=MetaWorkflowRun&field=uuid"
            + "".join(["&final_status=" + status for status in FINAL_STATUS_TO_KILL])
        )
        search_results = ff_utils.search_metadata(query, key=connection.ff_keys)
        get_and_add_uuids(search_results, meta_workflow_runs_to_kill)
    meta_workflow_runs_to_kill = list(set(meta_workflow_runs_to_kill))
    msg = "%s MetaWorkflowRun(s) found to stop" % len(meta_workflow_runs_to_kill)
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["found"] = meta_workflow_runs_to_kill
    if not meta_workflow_runs_to_kill:
        check.allow_action = False
    if meta_workflow_runs_not_found:
        check.status = "WARN"
        msg = "%s MetaWorkflowRuns were not found" % len(meta_workflow_runs_not_found)
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    return check


@action_function()
def kill_meta_workflow_runs(connection, **kwargs):
    """Stop MetaWorkflowRuns from further foursight checks/actions."""
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Stop MetaWorkflowfuns from further updates"

    success = []
    error = {}
    meta_workflow_runs_to_patch = check_result["found"]
    patch_body = {"final_status": "stopped"}
    for meta_workflow_run_uuid in meta_workflow_runs_to_patch:
        try:
            ff_utils.patch_metadata(
                patch_body, obj_id=meta_workflow_run_uuid, key=connection.ff_keys
            )
            success.append(meta_workflow_run_uuid)
        except Exception as error_msg:
            error[meta_workflow_run_uuid] = str(error_msg)
    action.output["success"] = success
    action.output["error"] = error
    return action


@check_function(
    meta_workflow="",
    cases=None,
    sample_processings=None,
)
def find_sample_processing_for_meta_workflow(
    connection,
    meta_workflow="",
    cases=None,
    sample_processings=None,
    **kwargs,
):
    """Find SampleProcessing items and MetaWorkflow to create new
    MetaWorkflowRun.
    """
    check = initialize_check(connection)
    check.description = (
        "Find SampleProcessing items and MetaWorkflow to create new MetaWorkflowRuns"
    )
    check.action = "create_meta_workflow_runs_for_items"

    sample_processings_for_meta_workflow = []
    if meta_workflow:
        meta_workflow_found, _ = validate_items_existence(meta_workflow, connection)
        if meta_workflow_found:
            meta_workflow_properties = meta_workflow_found[0]  # Only 1 MWF expected
            meta_workflow_name = meta_workflow_properties.get("name")
            meta_workflow = meta_workflow_properties.get("uuid")
            msg = "MetaWorkflow found: %s" % meta_workflow_name
            check.brief_output.append(msg)
            check.full_output["meta_workflow"] = meta_workflow
    else:
        msg = "MetaWorkflow not found: %s" % meta_workflow
        check.brief_output.append(msg)
        check.status = "WARN"
        check.allow_action = False
    if cases:
        cases_found = []
        cases_not_found = []
        cases_without_sample_processing = []
        cases = format_kwarg_list(cases)
        found, not_found = validate_items_existence(cases, connection)
        get_and_add_uuids(found, cases_found)
        cases_not_found += not_found
        for case_metadata in found:
            sample_processing = case_metadata.get("sample_processing")
            case_uuid = case_metadata.get("uuid")
            if sample_processing:
                sample_processings_for_meta_workflow.append(sample_processing)
            else:
                cases_without_sample_processing.append(case_uuid)
        msg = "%s Case(s) found" % len(cases_found)
        check.brief_output.append(msg)
        check.full_output["cases_found"] = cases_found
        if cases_not_found:
            msg = "%s Case(s) not found" % len(cases_not_found)
            check.status = "WARN"
            check.brief_output.append(msg)
            check.full_output["cases_not_found"] = cases_not_found
        if cases_without_sample_processing:
            msg = "%s Case(s) lacked a SampleProcessing" % len(
                cases_without_sample_processing
            )
            check.status = "WARN"
            check.brief_output.append(msg)
            check.full_output[
                "cases_without_sample_processing"
            ] = cases_without_sample_processing
    if sample_processings:
        sample_processings_found = []
        sample_processings = format_kwarg_list(sample_processings)
        found, not_found = validate_items_existence(sample_processings, connection)
        get_and_add_uuids(found, sample_processings_found)
        sample_processings_for_meta_workflow += sample_processings_found
        msg = "%s SampleProcessing(s) were found" % len(sample_processings_found)
        check.brief_output.append(msg)
        check.full_output["sample_processings_found"] = sample_processings_found
        if not_found:
            msg = "%s SampleProcessing(s) not found" % len(not_found)
            check.status = "WARN"
            check.brief_output.append(msg)
            check.full_output["sample_processings_not_found"] = not_found
    sample_processings_for_meta_workflow = list(
        set(sample_processings_for_meta_workflow)
    )
    msg = "%s SampleProcessing(s) found to use for MetaWorkflowRun creation" % len(
        sample_processings_for_meta_workflow
    )
    check.brief_output.append(msg)
    check.full_output[
        "sample_processing_for_meta_workflow"
    ] = sample_processings_for_meta_workflow
    if sample_processings_for_meta_workflow and meta_workflow:
        msg = "Action will create %s MetaWorkflowRun(s) for MetaWorkflow %s" % (
            len(sample_processings_for_meta_workflow),
            meta_workflow,
        )
        check.brief_output.append(msg)
        check.summary = msg
    else:
        msg = "Could not find information required to create MetaWorkflowRuns"
        check.status = "WARN"
        check.brief_output.append(msg)
        check.summary = msg
        check.allow_action = False
    return check


@action_function()
def create_meta_workflow_runs_for_items(connection, **kwargs):
    """Create MetaWorkflowRuns."""
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Create MetaWorkflowRuns"

    success = []
    error = {}
    meta_workflow_identifier = check_result.get("meta_workflow")
    sample_processings = check_result.get("sample_processing_for_meta_workflow", [])
    for sample_processing in sample_processings:
        try:
            MetaWorkflowRunFromSampleProcessing(
                sample_processing, meta_workflow_identifier, connection.ff_keys
            ).post_and_patch()
            success.append(sample_processing)
        except Exception as error_msg:
            error[sample_processing] = str(error_msg)
    action.output["sample_processings_with_meta_workflow_runs"] = success
    action.output["sample_processings_with_errors"] = error
    return action


@check_function(meta_workflow_runs=None)
def find_meta_workflow_runs_with_quality_metric_failure(
    connection, meta_workflow_runs=None, **kwargs
):
    """Find MetaWorkflowRuns with output QualityMetric failure(s)."""
    check = initialize_check(connection)
    check.action = "ignore_quality_metric_failure_for_meta_workflow_run"
    check.description = "Find MetaWorkflowRuns with output QualityMetric failure(s)"

    meta_workflow_runs_found = []
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        for meta_workflow_run in found:
            final_status = meta_workflow_run.get("final_status")
            if final_status == "quality metric failed":
                meta_workflow_run_uuid = meta_workflow_run.get("uuid")
                meta_workflow_runs_found.append(meta_workflow_run_uuid)
        meta_workflow_runs_not_found += not_found
    else:
        query = (
            "search/?type=MetaWorkflowRun&field=uuid&final_status=quality+metric+failed"
        )
        search = ff_utils.search_metadata(query, key=connection.ff_keys)
        get_and_add_uuids(search, meta_workflow_runs_found)
    msg = "%s MetaWorkflowRun(s) found with failed output QualityMetrics" % len(
        meta_workflow_runs_found
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["failing_quality_metrics"] = meta_workflow_runs_found
    if meta_workflow_runs_found:
        check.status = "WARN"
    else:
        check.allow_action = False
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRun(s) could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    return check


@action_function()
def ignore_quality_metric_failure_for_meta_workflow_run(connection, **kwargs):
    """Ignore output QualityMetric failures on MetaWorkflowRuns to
    allow MetaWorkflowRuns to continue.
    """
    action, check_result = initialize_action(connection, kwargs)
    action.description = "Ignore MetaWorkflowRun QC failures to continue running"

    success = []
    error = {}
    meta_workflow_run_uuids = check_result.get("failing_quality_metrics", [])
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        patch_body = {"ignore_output_quality_metrics": True, "final_status": "running"}
        try:
            ff_utils.patch_metadata(
                patch_body, meta_workflow_run_uuid, key=connection.ff_keys
            )
            success.append(meta_workflow_run_uuid)
        except Exception as error_msg:
            error[meta_workflow_run_uuid] = str(error_msg)
    action.output["success"] = success
    action.output["error"] = error
    return action
