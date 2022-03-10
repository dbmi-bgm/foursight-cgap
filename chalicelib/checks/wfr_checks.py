import inspect
import json
import random
from datetime import datetime

from dcicutils import ff_utils, s3Utils
from magma_ff import reset_metawfr, run_metawfr, status_metawfr
from magma_ff.create_metawfr import (
    MetaWorkflowRunCreationError,
    MetaWorkflowRunFromSampleProcessing,
)
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

META_WORKFLOW_RUN_STATUS_TO_RUN = ["running", "inactive", "pending"]
META_WORKFLOW_RUN_STATUS_TO_CHECK = ["running"]


# TODO: Add permitted status args to pass to magma for running, checkstatus, failed
# restarts since grabbing datastore=database there to get around indexing queue issue

# TODO: Figure out line_count check with Michele --> should really be a QualityMetric
# produced by MWFR

# TODO: File type changes need to be accounted for or reverted


def initialize_check(connection):
    """"""
    function_called_by = inspect.stack()[1].function
    check = CheckResult(connection, function_called_by)
    check.brief_output = []
    check.full_output = {}
    check.status = "PASS"
    check.allow_action = True
    return check


def initialize_action(connection, kwargs):
    """"""
    function_called_by = inspect.stack()[1].function
    action = ActionResult(connection, function_called_by)
    action.status = "DONE"
    action.output = {}
    check_result = action.get_associated_check_result(kwargs).get("full_output", {})
    return action, check_result


def validate_items_existence(item_identifiers, connection):
    """"""
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
    """"""
    existing_item_value = dictionary.get(key)
    if existing_item_value:
        existing_item_value.append(value)
    else:
        dictionary[key] = [value]


def get_and_add_uuids(items, list_to_append):
    """"""
    for item in items:
        item_uuid = item.get("uuid")
        if item_uuid is not None:
            list_to_append.append(item_uuid)


def make_embed_request(ids, fields, connection):
    """"""
    if isinstance(ids, str):
        ids = [ids]
    if isinstance(fields, str):
        fields = [fields]
    post_body = {"ids": ids, "fields": fields}
    endpoint = connection.ff_server + "embed"
    result = ff_utils.authorized_request(
        endpoint, verb="POST", auth=connection.ff_keys, data=json.dumps(post_body)
    ).json()
    if len(result) == 1:
        result = result[0]
    return result


def nested_getter(item, fields):
    """"""
    result = []
    if isinstance(fields, str):
        fields = fields.split(".")
    field_to_get = fields.pop(0)
    if isinstance(item, dict):
        field_value = item.get(field_to_get)
        if field_value is not None:
            result = field_value
    elif isinstance(item, list):
        for sub_item in item:
            sub_result = nested_getter(sub_item, field_to_get)
            if sub_result == []:
                continue
            else:
                result.append(sub_result)
    if fields and result:
        result = nested_getter(result, fields)
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
        check.allow_action = True
        check.summary = "Some files are missing md5 runs"
        msg = str(len(missing_md5)) + " file(s) lack a successful md5 run"
        check.brief_output.append(msg)
        check.full_output["files_without_md5run"] = missing_md5
        check.status = "WARN"
    if not_switched_status:
        check.allow_action = True
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
    action.status = "DONE"

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

    my_auth = connection.ff_keys
    query = (
        "search/?type=MetaWorkflowRun&overall_qcs.name!=linecount_test"
        "&final_status=completed&field=uuid&field=title"
    )
    need_linecount_test = ff_utils.search_metadata(query, key=my_auth)
    metawfr_uuids = [r["uuid"] for r in need_linecount_test]
    metawfr_titles = [r["title"] for r in need_linecount_test]
    msg = "%s MetaWorkflowRuns may require line count checks." % len(
        need_linecount_test
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.status = "WARN"
    check.full_output["metawfrs_to_run"] = {
        "titles": metawfr_titles,
        "uuids": metawfr_uuids,
    }
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
    env = connection.ff_env
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
    # we want to display an error if there are any errors in the run, even if many patches are successful
    if action_logs["error"] == []:
        action.status = "DONE"
    else:
        action.status = "ERROR"

    return action


@check_function()
def metawfrs_to_run(connection, **kwargs):
    """
    Find metaworkflowruns that may need to run steps
    - those with final_status 'pending', 'inactive' and 'running'

    'pending' means no workflow run has started
    'inactive' means some workflow runs are completed but others are pending
    'running' means some workflow runs are actively running

    note: the check is currently looking also for 'failed' metaworkflowruns,
          we probably want to disable this
    """
    check = initialize_check(connection)
    my_auth = connection.ff_keys
    check.action = "run_metawfrs"
    check.description = "Find metaworkflow runs that has workflow runs to be kicked."
    check.summary = ""

    query = "/search/?type=MetaWorkflowRun" + "".join(
        ["&final_status=" + st for st in ["pending", "inactive", "running"]]
    )
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run, return
    if not search_res:
        check.summary = "All Good!"
        return check

    metawfr_uuids = [r["uuid"] for r in search_res]
    metawfr_titles = [r["title"] for r in search_res]

    check.summary = "Some metawfrs may have wfrs to be kicked."
    check.status = "WARN"
    msg = str(len(metawfr_uuids)) + " metawfrs may have wfrs to be kicked"
    check.brief_output.append(msg)
    check.full_output["metawfrs_to_run"] = {
        "titles": metawfr_titles,
        "uuids": metawfr_uuids,
    }
    return check


@action_function()
def run_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action, check_result = intialize_action(connection, kwargs)

    action_logs = {"runs_checked_or_kicked": []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    sfn = "tibanna_zebra_" + env.replace("fourfront-", "")
    action_logs["check_output"] = check_result
    metawfr_uuids = check_result.get("metawfrs_to_run", {}).get("uuids", [])
    random.shuffle(
        metawfr_uuids
    )  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            run_metawfr.run_metawfr(
                metawfr_uuid, my_auth, verbose=False, sfn=sfn, env=env
            )
            action_logs["runs_checked_or_kicked"].append(metawfr_uuid)
        except Exception as e:
            action_logs["error"] = str(e)
            break
    action.output = action_logs
    return action


@check_function()
def metawfrs_to_checkstatus(connection, **kwargs):
    """
    Find metaworkflowruns that may need status-checking
    - those with final_status 'running'

    'running' means some workflow runs are actively running
    """
    check = initialize_check(connection)
    my_auth = connection.ff_keys
    check.action = "checkstatus_metawfrs"
    check.description = (
        "Find metaworkflow runs that has workflow runs to be status-checked."
    )
    check.summary = ""

    query = "/search/?type=MetaWorkflowRun" + "".join(
        ["&final_status=" + st for st in ["running"]]
    )
    search_res = ff_utils.search_metadata(query, key=my_auth)
    if not search_res:
        check.summary = "All Good!"
        return check
    metawfr_uuids = [r["uuid"] for r in search_res]
    metawfr_titles = [r["title"] for r in search_res]
    check.allow_action = True
    check.summary = "Some metawfrs may have wfrs to be status-checked."
    check.status = "WARN"
    msg = str(len(metawfr_uuids)) + " metawfrs may have wfrs to be status-checked"
    check.brief_output.append(msg)
    check.full_output["metawfrs_to_check"] = {
        "titles": metawfr_titles,
        "uuids": metawfr_uuids,
    }
    return check


@action_function()
def checkstatus_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action, check_result = initialize_action(connection, kwargs)

    action_logs = {"runs_checked": []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    action_logs["check_output"] = check_result
    metawfr_uuids = check_result.get("metawfrs_to_check", {}).get("uuids", [])
    random.shuffle(
        metawfr_uuids
    )  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            status_metawfr.status_metawfr(metawfr_uuid, my_auth, verbose=False, env=env)
            action_logs["runs_checked"].append(metawfr_uuid)
        except Exception as e:
            action_logs["error"] = str(e)
            break
    action.output = action_logs
    action.status = "DONE"
    return action


@check_function()
def spot_failed_metawfrs(connection, **kwargs):
    """
    Find metaworkflowruns that failed
    - those with status 'failed'

    Reset status to pending if it is spot interruption
    """
    check = initialize_check(connection)
    my_auth = connection.ff_keys
    check.action = "reset_spot_failed_metawfrs"
    check.description = "Find metaworkflow runs that has failed workflow runs that may be due to spot interruption."
    check.summary = ""

    env = connection.ff_env
    query = "/search/?type=MetaWorkflowRun" + "".join(
        ["&final_status=" + st for st in ["failed"]]
    )
    search_res = ff_utils.search_metadata(query, key=my_auth)
    if not search_res:
        check.summary = "All Good!"
        return check
    metawfr_uuids = [r["uuid"] for r in search_res]
    metawfr_titles = [r["title"] for r in search_res]
    check.summary = "Some metawfrs have failed wfrs."
    check.status = "WARN"
    msg = str(len(metawfr_uuids)) + " metawfrs may have failed wfrs"
    check.brief_output.append(msg)
    check.full_output["metawfrs_that_failed"] = {
        "titles": metawfr_titles,
        "uuids": metawfr_uuids,
    }
    return check


@action_function()
def reset_spot_failed_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action, check_result = initialize_action(connection, kwargs)

    action_logs = {"runs_reset": []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    my_s3_util = s3Utils(env=env)
    log_bucket = my_s3_util.tibanna_output_bucket
    action_logs["check_output"] = check_result
    metawfr_uuids = check_result.get("metawfrs_that_failed", {}).get("uuids", [])
    random.shuffle(
        metawfr_uuids
    )  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            metawfr_meta = ff_utils.get_metadata(
                metawfr_uuid, key=my_auth, add_on="frame=raw&datastore=database"
            )
            shards_to_reset = []
            for wfr in metawfr_meta["workflow_runs"]:
                if wfr["status"] == "failed":
                    res = ff_utils.search_metadata(
                        "/search/?type=WorkflowRunAwsem&awsem_job_id=%s" % wfr["jobid"],
                        key=my_auth,
                    )
                    if len(res) == 1:
                        res = res[0]
                    elif len(res) > 1:
                        raise Exception(
                            "multiple workflow runs for job id %s" % wfr["jobid"]
                        )
                    else:
                        raise Exception(
                            "No workflow run found for job id %s" % wfr["jobid"]
                        )
                    # If Tibanna received a spot termination notice, it will create the file JOBID.spot_failure in the
                    # Tibanna log bucket. If it failed otherwise it will throw an EC2UnintendedTerminationException
                    # which will create a corresponding entry in the workflow description
                    if (
                        my_s3_util.does_key_exist(
                            key=wfr["jobid"] + ".spot_failure",
                            bucket=log_bucket,
                            print_error=False,
                        )
                        or "EC2 unintended termination" in res.get("description", "")
                        or "EC2 Idle error" in res.get("description", "")
                    ):
                        # reset spot-failed shards
                        shard_name = wfr["name"] + ":" + str(wfr["shard"])
                        shards_to_reset.append(shard_name)
            reset_metawfr.reset_shards(
                metawfr_uuid, shards_to_reset, my_auth, verbose=False
            )
            action_logs["runs_reset"].append(
                {"metawfr": metawfr_uuid, "shards": shards_to_reset}
            )
        except Exception as e:
            action_logs["error"] = str(e)
            break
    action.output = action_logs
    return action


@check_function(meta_workflow_runs=None)
def failed_metawfrs(connection, meta_workflow_runs=None, **kwargs):
    """
    Find metaworkflowruns that are failed
    - those with status 'failed'

    Reset status to pending all
    """
    check = initialize_check(connection)
    check.action = "reset_failed_metawfrs"
    check.description = "Find MetaWorkflowRuns that have failed workflow runs."

    meta_workflow_run_uuids = []
    meta_workflow_run_titles = []
    if meta_workflow_runs:
        meta_workflow_runs_found, _ = validate_items_existence(
            meta_workflow_runs, connection
        )
        titles = [x["title"] for x in meta_workflow_runs_found]
        uuids = [x["uuid"] for x in meta_workflow_runs_found]
        meta_workflow_run_titles += titles
        meta_workflow_run_uuids += uuids
    else:
        query = "/search/?type=MetaWorkflowRun&final_status=failed"
        search_res = ff_utils.search_metadata(query, key=connection.ff_keys)
        uuids = [r["uuid"] for r in search_res]
        titles = [r["title"] for r in search_res]
        meta_workflow_run_titles += titles
        meta_workflow_run_uuids += uuids
    if not meta_workflow_run_uuids:
        check.allow_action = False
    msg = "%s MetaWorkflowRuns have failed WorkflowRuns" % len(meta_workflow_run_uuids)
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["failed_meta_workflow_runs"] = {
        "titles": meta_workflow_run_titles,
        "uuids": meta_workflow_run_uuids,
    }
    return check


@action_function()
def reset_failed_metawfrs(connection, **kwargs):
    action, check_result = initialize_action(connection, kwargs)

    runs_reset = []
    errors = {}
    start = datetime.utcnow()
    meta_workflow_run_uuids = check_result.get("failed_meta_workflow_runs", {}).get(
        "uuids", []
    )
    random.shuffle(meta_workflow_run_uuids)
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        now = datetime.utcnow()
        if (now - start).seconds > lambda_limit:
            action.description = "Did not complete action due to time limitations"
            break
        try:
            reset_metawfr.reset_failed(meta_workflow_run_uuid, connection.ff_keys)
            runs_reset.append(meta_workflow_run_uuid)
        except Exception as e:
            errors[meta_workflow_run_uuid] = str(e)
    action.output["runs_reset"] = runs_reset
    if errors:
        action.status = "WARN"
        action.output["errors"] = errors
    return action


@check_function(start_date=None, file_accessions="")
def ingest_vcf_status(connection, **kwargs):
    """
    Search for full annotated VCF files that needs to be ingested

    kwargs:
        start_date -- limit search to files generated since a date formatted YYYY-MM-DD
        file_accession -- run check with given files instead of the default query
                          expects comma/space separated accessions
    """
    ### General check attributes
    check = CheckResult(connection, "ingest_vcf_status")
    my_auth = connection.ff_keys
    check.action = "ingest_vcf_start"
    check.brief_output = []
    check.full_output = {}
    check.status = "PASS"
    check.allow_action = False

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = "PASS"  # maybe use warn?
        check.brief_output = ["Waiting for indexing queue to clear"]
        check.summary = "Waiting for indexing queue to clear"
        check.full_output = {}
        return check

    # basic query (skip to be uploaded by workflow)
    query = (
        "/search/?file_type=full+annotated+VCF&type=FileProcessed"
        "&file_ingestion_status=No value&file_ingestion_status=N/A"
        "&status!=uploading&status!=to be uploaded by workflow&status!=upload failed"
    )
    s_date = kwargs.get("start_date")
    if s_date:
        query += "&date_created.from=" + s_date
    file_accessions = kwargs.get("file_accessions")
    if file_accessions:
        file_accessions = file_accessions.replace(" ", ",")
        accessions = [i.strip() for i in file_accessions.split(",") if i]
        for an_acc in accessions:
            query += "&accession={}".format(an_acc)
    results = ff_utils.search_metadata(query, key=my_auth)
    if not results:
        check.summary = "All Good!"
        return check
    msg = "{} files will be added to the ingestion_queue".format(str(len(results)))
    files = [i["uuid"] for i in results]
    check.status = "WARN"  # maybe use warn?
    check.brief_output = [
        msg,
    ]
    check.summary = msg
    check.full_output = {
        "files": files,
        "accessions": [i["accession"] for i in results],
    }
    return check


@action_function()
def ingest_vcf_start(connection, **kwargs):
    """
    Start ingest_vcf runs by sending compiled input_json to run_workflow endpoint
    """
    action = ActionResult(connection, "ingest_vcf_start")
    action_logs = {"runs_started": [], "runs_failed": []}
    my_auth = connection.ff_keys
    ingest_vcf_check_result = action.get_associated_check_result(kwargs).get(
        "full_output", {}
    )
    targets = ingest_vcf_check_result["files"]
    post_body = {"uuids": targets}
    action_logs = ff_utils.post_metadata(post_body, "/queue_ingestion", my_auth)
    action.output = action_logs
    action.status = "DONE"

    return action


@check_function(file_accessions="")
def check_vcf_ingestion_errors(connection, **kwargs):
    """
    Check for finding full annotated VCFs that have failed ingestion, so that they
    can be reset and the ingestion rerun if needed.
    """
    check = CheckResult(connection, "check_vcf_ingestion_errors")
    accessions = [
        accession.strip()
        for accession in kwargs.get("file_accessions", "").split(",")
        if accession
    ]
    ingestion_error_search = "search/?file_type=full+annotated+VCF&type=FileProcessed&file_ingestion_status=Error"
    if accessions:
        ingestion_error_search += "&accession="
        ingestion_error_search += "&accession=".join(accessions)
    ingestion_error_search += "&field=@id&field=file_ingestion_error"
    results = ff_utils.search_metadata(ingestion_error_search, key=connection.ff_keys)
    output = {}
    for result in results:
        if len(result.get("file_ingestion_error")) > 0:
            # usually there are 100 errors, so just report first error, user can view item to see others
            output[result["@id"]] = result["file_ingestion_error"][0].get("body")
    check.full_output = output
    check.brief_output = list(output.keys())
    if output:
        check.status = "WARN"
        check.summary = f"{len(check.brief_output)} VCFs failed ingestion"
        check.description = check.summary
        check.allow_action = True
        check.action = ""
    else:
        check.status = "PASS"
        check.summary = "No VCFs found with ingestion errors"
        check.description = check.summary
    return check


@action_function()
def reset_vcf_ingestion_errors(connection, **kwargs):
    """
    Takes VCFs with ingestion errors, patches file_ingestion_status to 'N/A', and
    removes file_ingestion_error property. This will allow ingestion to be retried.
    """
    action = ActionResult(connection, "reset_vcf_ingestion_errors")
    check_result_vcfs = action.get_associated_check_result(kwargs).get(
        "brief_output", []
    )
    action_logs = {"success": [], "fail": {}}
    for vcf in check_result_vcfs:
        patch = {"file_ingestion_status": "N/A"}
        try:
            resp = ff_utils.patch_metadata(
                patch,
                vcf + "?delete_fields=file_ingestion_error",
                key=connection.ff_keys,
            )
        except Exception as e:
            action_logs["fail"][vcf] = str(e)
        else:
            if resp["status"] == "success":
                action_logs["success"].append(vcf)
            else:
                action_logs["fail"][vcf] = resp["status"]
    action.output = action_logs
    if action_logs["fail"]:
        action.status = "ERROR"
    else:
        action.status = "DONE"
    return action


@check_function()
def find_meta_workflow_runs_requiring_output_linktos(connection, **kwargs):
    """"""
    check = initialize_check(connection)
    check.description = "Find completed MetaWorkflowRuns to PATCH their output files."
    check.action = "link_meta_workflow_run_output_files"
    check.action_message = "Add MetaWorkflowRuns' output files to desired locations."

    meta_workflow_runs_found = []
    query = (
        "search/?type=MetaWorkflowRun&final_status=completed&field=uuid"
        "&output_files_linked_status=No+value"
    )
    search = ff_utils.search_metadata(query, key=connection.ff_keys)
    for item in search:
        meta_workflow_run_uuid = item.get("uuid")
        meta_workflow_runs_found.append(meta_workflow_run_uuid)
    msg = "%s MetaworkflowRuns found to PATCH output files" % len(
        meta_workflow_runs_found
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = meta_workflow_runs_found
    if not meta_workflow_runs_found:
        check.allow_action = False
    return check


@action_function()
def link_meta_workflow_run_output_files(connection, **kwargs):
    """"""
    action, check_result = initialize_action(connection, kwargs)

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
    """"""
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
    """
    Currently same for both Sample and SampleProcessing.
    If new item or location to patch is added, will need to update schema
    and refactor here accordingly.
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
                import pdb

                pdb.set_trace()
                for file_uuid in files_to_patch:
                    add_to_dict_as_list(linkto_errors, file_uuid, str(error_msg))
    return linkto_errors


def update_meta_workflow_run_files_linked(
    meta_workflow_run_uuid, errors=None, connection=None
):
    """"""
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
    """"""
    check = initialize_check(connection)
    check.summary = ""
    check.description = ""
    check.action = "link_meta_workflow_run_output_files_after_error"
    check.action_message = ""

    meta_workflow_runs_found = []
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
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
            "&output_files_linked_errors.output_file!=No+value"
        )
        search = ff_utils.search_metadata(query, key=connection.ff_keys)
        for item in search:
            meta_workflow_run_uuid = item.get("uuid")
            meta_workflow_runs_found.append(meta_workflow_run_uuid)
    msg = "%s MetaWorkflowRuns found with errors for output file links" % len(
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
        msg = "%s MetaWorkflowRuns could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    return check


@action_function()
def link_meta_workflow_run_output_files_after_error(connection, **kwargs):
    """"""
    action, check_result = initialize_action(connection, kwargs)

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
    """"""
    check = initialize_check(connection)
    check.description = 'Find MetaWorkflowRuns to set final_status to "stopped"'
    check.action = "kill_meta_workflow_runs"

    meta_workflow_runs_to_kill = []
    meta_workflow_runs_not_found = []
    if meta_workflow_run_uuids is not None:
        (
            meta_workflow_runs_found,
            meta_workflow_runs_not_found,
        ) = validate_items_existence(meta_workflow_run_uuids, connection)
        get_and_add_uuids(meta_workflow_runs_found, meta_workflow_runs_to_kill)
    if meta_workflow_uuids is not None:
        for meta_workflow_uuid in meta_workflow_uuids:
            search_query = (
                "search/?type=MetaWorkflowRun&field=uuid&meta_workflow.uuid="
                + meta_workflow_uuid
                + "".join(
                    [
                        "&final_status=" + status
                        for status in ["pending", "inactive", "running", "failed"]
                    ]
                )
            )
            search_results = ff_utils.search_metadata(
                search_query, key=connection.ff_keys
            )
            get_and_add_uuids(search_results, meta_workflow_runs_to_kill)
    msg = "%s MetaWorkflowRuns found" % len(meta_workflow_runs_to_kill)
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["found"] = meta_workflow_runs_to_kill
    if meta_workflow_runs_not_found:
        check.status = "WARN"
        msg = "%s MetaWorkflowRuns were not found" % len(meta_workflow_runs_not_found)
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    return check


@action_function()
def kill_meta_workflow_runs(connection, **kwargs):
    """"""
    action, check_results = initialize_action(connection, kwargs)
    action.description = 'PATCH MetaWorkflowRuns\' final_status to "stopped"'

    patch_success = []
    patch_failure = []
    meta_workflow_runs_to_patch = check_results["found"]
    patch_body = {"final_status": "stopped"}
    for meta_workflow_run_uuid in meta_workflow_runs_to_patch:
        try:
            ff_utils.patch_metadata(
                patch_body, obj_id=meta_workflow_run_uuid, key=connection.ff_keys
            )
            patch_success.append(meta_workflow_run_uuid)
        except Exception as error_msg:
            patch_failure.append({meta_workflow_run_uuid: str(error_msg)})
    action.output = {"patch_success": patch_success, "patch_failure": patch_failure}
    return action


@check_function(
    meta_workflow_identifier="",
    case_identifiers=None,
    sample_processing_identifiers=None,
)
def find_sample_processing_for_meta_workflow(
    connection,
    meta_workflow_identifier="",
    case_identifiers=None,
    sample_processing_identifiers=None,
    **kwargs,
):
    """"""
    check = initialize_check(connection)
    check.description = ""
    check.action = "create_meta_workflow_runs_for_items"

    sample_processings = []
    meta_workflow_found, _ = validate_items_existence(
        meta_workflow_identifier, connection
    )
    if meta_workflow_found:
        msg = "MetaWorkflow found"
        check.brief_output.append(msg)
        check.full_output["meta_workflow"] = meta_workflow_identifier
    else:
        msg = "MetaWorkflow %s not found" % meta_workflow_identifier
        check.brief_output.append(msg)
        check.status = "WARN"
        check.full_output["meta_workflow"] = None
    if case_identifiers is not None:
        cases_found, cases_not_found = validate_items_existence(
            case_identifiers, connection
        )
        cases_without_sample_processing = []
        for case_metadata in cases_found:
            sample_processing = case_metadata.get("sample_processing")
            if sample_processing:
                sample_processings.append(sample_processing)
            else:
                cases_without_sample_processing.append(case_metadata)
        cases_found_uuids = []
        get_and_add_uuids(cases_found, cases_found_uuids)
        msg = "%s Cases were found" % len(cases_found)
        check.brief_output.append(msg)
        check.full_output["cases_found"] = cases_found_uuids
        if cases_not_found:
            not_found_uuids = nested_getter(cases_not_found, "uuid")
            msg = "%s Cases were not found" % len(cases_not_found)
            check.status = "WARN"
            check.brief_output.append(msg)
            check.full_output["cases_not_found"] = not_found_uuids
        if cases_without_sample_processing:
            cases_without_sample_processing_uuids = []
            get_and_add_uuids(
                cases_without_sample_processing, cases_without_sample_processing_uuids
            )
            msg = "%s Cases lacked a SampleProcessing" % len(
                cases_without_sample_processing
            )
            check.status = "WARN"
            check.brief_output.append(msg)
            check.full_output[
                "cases_without_sample_processing"
            ] = cases_without_sample_processing_uuids
    if sample_processing_identifiers is not None:
        sample_processing_found_uuids = []
        sample_processing_found, sample_processing_not_found = validate_items_existence(
            sample_processing_identifiers, connection
        )
        get_and_add_uuids(sample_processing_found, sample_processing_found_uuids)
        sample_processings += sample_processing_found_uuids
        msg = "%s SampleProcessings were found" % len(sample_processing_found_uuids)
        check.brief_output.append(msg)
        check.full_output["sample_processings_found"] = sample_processing_found_uuids
        if sample_processing_not_found:
            sample_processing_not_found = []
            get_and_add_uuids(sample_processing_not_found, sample_processing_not_found)
            msg = "%s SampleProcessings were not found" % len(
                sample_processing_not_found
            )
            check.status = "WARN"
            check.brief_output.append(msg)
            check.full_output[
                "sample_processings_not_found"
            ] = sample_processing_not_found
    msg = "%s SampleProcessings to use for MetaWorkflowRun creation" % len(
        sample_processings
    )
    check.brief_output.append(msg)
    check.full_output["sample_processing_for_meta_workflow"] = sample_processings
    if sample_processings and meta_workflow_found:
        msg = "Action will create %s MetaWorkflowRuns for MetaWorkflow %s" % (
            len(sample_processings),
            meta_workflow_identifier,
        )
        check.brief_output.append(msg)
        check.summary = msg
    else:
        msg = "Could not find information required to create MetaWorkflowRuns"
        check.brief_output.append(msg)
        check.summary = msg
        check.allow_action = False
    return check


@action_function()
def create_meta_workflow_runs_for_items(connection, **kwargs):
    """"""
    action, check_results = initialize_action(connection, kwargs)
    action.description = ""

    sample_processing_success = []
    sample_processing_error = []
    meta_workflow_identifier = check_results.get("meta_workflow")
    sample_processings = check_results.get("sample_processing_for_meta_workflow", [])
    for sample_processing in sample_processings:
        try:
            MetaWorkflowRunFromSampleProcessing(
                sample_processing, meta_workflow_identifier, connection.ff_keys
            ).post_meta_workflow_run_and_patch_sample_processing()
            sample_processing_success.append(sample_processing)
        except Exception as error_msg:
            sample_processing_error.append({sample_processing: str(error_msg)})
    action.output[
        "sample_processings_with_meta_workflow_runs"
    ] = sample_processing_success
    action.output["sample_processings_with_errors"] = sample_processing_error
    return action


@check_function(meta_workflow_runs=None)
def find_meta_workflow_runs_with_quality_metric_failure(
    connection, meta_workflow_runs=None, **kwargs
):
    """"""
    check = initialize_check(connection)
    check.action = ""
    check.description = ""

    meta_workflow_runs_found = []
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        for meta_workflow_run in found:
            final_status = meta_workflow_run.get("final_status")
            if final_status == "quality metric failed":
                uuid = meta_workflow_run.get("uuid")
                meta_workflow_runs_found.append("uuid")
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
    check.full_output["failing_quality_metrics"]
    if meta_workflow_runs_found:
        check.status = "WARN"
    else:
        check.allow_action = False
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRuns could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    return check


@action_function
def ignore_quality_metric_failure_for_meta_workflow_run(connection, **kwargs):
    """"""
    action, check_result = initialize_action(connection, kwargs)

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


@check_function(case_identifiers=None)
def find_cases_for_cram_conversion_metaworkflowruns(
    connection, case_identifiers=None, **kwargs
):
    """"""
    check = initialize_check(connection)
    check.description = ""

    if case_identifiers:
        cases_found, cases_not_found = validate_items_existence(
            case_identifiers, connection
        )
    else:
        samples_query = (
            "search/?type=Sample&field=uuid&cram_files.status=Uploaded"
            "&processed_files.file_format.file_format!=fastq"
        )
        samples_search = ff_utils.search_metadata(samples_query, key=connection.ff_keys)
        if samples_search:
            sample_processings = []
            sample_uuids = set(nested_getter(samples_search, "uuid"))
            for sample_uuid in sample_uuids:
                sample_processing_query = (
                    "search/?type=SampleProcessing&field=uuid&field=samples.uuid"
                    "&samples.uuid=" + sample_uuid
                )
                sample_processing_search = ff_utils.search_metadata(
                    sample_processing_query, key=connection.ff_keys
                )
                for sample_processing in sample_processing_search:
                    sample_processing_uuid = sample_processing.get("uuid")
                    sample_processing_samples_uuids = set(
                        nested_getter(sample_processing, "samples.uuid")
                    )
                    uuid_intersection = sample_uuids.intersect(
                        sample_processing_samples_uuids
                    )
                    if uuid_intersection == sample_processing_samples_uuids:
                        sample_processings.append(sample_processing_uuid)


@action_function()
def create_case_metaworkflowruns_for_cram_conversion(connection, **kwargs):
    """"""


@check_function()
def find_cases_for_upstream_metaworkflowruns(connection, **kwargs):
    """"""


@action_function()
def create_case_upstream_metaworkflowruns(connection, **kwargs):
    """"""


@check_function()
def find_cases_for_snv_metaworkflowruns():
    """"""


@action_function()
def create_case_snv_metaworkflowruns():
    """"""


@check_function()
def find_cases_for_sv_metaworkflowruns():
    """"""


@action_function()
def create_case_sv_metaworkflowruns():
    """"""
