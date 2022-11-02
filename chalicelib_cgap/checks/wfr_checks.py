import random
import re
from datetime import datetime

from dcicutils import ff_utils
from dcicutils.s3_utils import s3Utils
from magma_ff import reset_metawfr, run_metawfr, status_metawfr
from magma_ff.create_metawfr import (
    MetaWorkflowRunFromSampleProcessing,
    MetaWorkflowRunFromSample,
)

from .helpers import constants
from .helpers import wfr_utils
from .helpers.confchecks import action_function, check_function
from .helpers.utils import (
    initialize_check,
    initialize_action,
    format_kwarg_list,
    validate_items_existence,
    add_to_dict_as_list,
    make_embed_request,
    get_step_function_name,
    is_past_time_limit,
)
from .helpers.wfrset_utils import LAMBDA_LIMIT


# TODO: Figure out line_count check with Michele --> should really be a QualityMetric
# produced by MWFR

FINAL_STATUS_TO_RUN = [
    constants.MWFR_RUNNING,
    constants.MWFR_INACTIVE,
    constants.MWFR_PENDING,
]
FINAL_STATUS_TO_CHECK = [constants.MWFR_RUNNING]
FINAL_STATUS_TO_RESET = [constants.MWFR_FAILED]
FINAL_STATUS_TO_KILL = [
    constants.MWFR_RUNNING,
    constants.MWFR_INACTIVE,
    constants.MWFR_PENDING,
    constants.MWFR_FAILED,
]
SPOT_FAILURE_DESCRIPTIONS = ["EC2 unintended termination", "EC2 Idle error"]


class MetaWorkflowRunsFound:
    """Helper class to hold MetaWorkflowRuns' information."""

    def __init__(self, connection):
        self.key = connection.ff_keys
        self.uuids = []
        self.titles = []

    def add_items(self, meta_workflow_runs):
        """Grab UUIDs and titles and update attributes.

        :param meta_workflow_runs: MetaWorkflowRuns' properties
        :type meta_workflow_runs: list(dict)
        """
        for meta_workflow_run in meta_workflow_runs:
            uuid = meta_workflow_run.get("uuid")
            self.uuids.append(uuid)
            title = meta_workflow_run.get("title")
            self.titles.append(title)

    def search_final_status(self, final_status):
        """Find MetaWorkflowRuns matching final status values and
        update attributes

        :param final_status: Valid final_status values
        :type final_status: list(str)
        """
        query = "/search/?type=MetaWorkflowRun&field=uuid&field=title"
        query += "".join("&final_status=" + status for status in final_status)
        self.search_query(query)

    def search_query(self, query):
        """Find MetaWorkflowRuns matching the query and update
        attributes.

        :param query: Search query
        :type query: str
        """
        search_response = ff_utils.search_metadata(query, key=self.key)
        self.add_items(search_response)


@check_function(file_type="File", start_date=None)
def md5runCGAP_status(connection, file_type="", start_date=None, **kwargs):
    """Find files uploaded to S3 without MD5 checksum

    Check assumptions:
        - all files that have a status uploaded run through md5runCGAP
        - all files status uploading/upload failed and NO s3 file are
            skipped

    kwargs:
        file_type -- limit search to a file type, i.e. FileFastq
        start_date -- limit search to files generated since date,
            formatted as YYYY-MM-DD
    """
    start = datetime.utcnow()
    check = initialize_check("md5runCGAP_status", connection)
    check.action = "md5runCGAP_start"
    check.description = "Find files uploaded to S3 without MD5 checksum"

    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=False)
    if indexing_queue:
        check.status = constants.CHECK_PASS
        check.brief_output = ["Waiting for indexing queue to clear"]
        check.summary = "Waiting for indexing queue to clear"
        check.allow_action = False
        return check
    my_auth = connection.ff_keys
    query = "/search/?status=uploading&status=upload failed"
    query += "&type=" + file_type
    if start_date is not None:
        query += "&date_created.from=" + start_date
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.status = constants.CHECK_PASS
        check.summary = "All Good!"
        check.allow_action = False
        return check
    no_s3_file = []
    running = []
    missing_md5 = []
    not_switched_status = []
    problems = []  # multiple failed runs
    my_s3_util = s3Utils(env=env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket
    for a_file in res:
        if is_past_time_limit(start, LAMBDA_LIMIT):
            check.brief_output.append("Did not complete due to time limitations")
            break
        # find bucket
        if "FileProcessed" in a_file["@type"]:
            my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference
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
        msg = "%s file(s) are pending upload" % len(no_s3_file)
        check.brief_output.append(msg)
        check.full_output["files_pending_upload"] = no_s3_file
    if running:
        msg = str(len(running)) + " file(s) have MD5 checksum running"
        check.brief_output.append(msg)
        check.full_output["files_running_md5"] = running
    if problems:
        msg = str(len(problems)) + " file(s) have problems"
        check.brief_output.append(msg)
        check.full_output["problems"] = problems
    if missing_md5:
        msg = str(len(missing_md5)) + " file(s) lack a successful MD5 run"
        check.brief_output.append(msg)
        check.full_output["files_without_md5run"] = missing_md5
    if not_switched_status:
        msg = (
            str(len(not_switched_status))
            + " file(s) completed MD5 checksum and require status update"
        )
        check.brief_output.append(msg)
        check.full_output["files_with_run_and_wrong_status"] = not_switched_status
    action_items = missing_md5 + not_switched_status
    msg = "%s file(s) require MD5 checksum start or status update" % len(action_items)
    check.summary = msg
    if not action_items:
        check.allow_action = False
    if not action_items and not problems:
        check.status = constants.CHECK_PASS
    return check


@action_function(start_missing=True, start_not_switched=True)
def md5runCGAP_start(connection, start_missing=True, start_not_switched=True, **kwargs):
    """Start MD5 checksums on Files or update File MD5 checksum status"""
    start = datetime.utcnow()
    action, check_result = initialize_action("md5runCGAP_start", connection, kwargs)

    targets = []
    runs_started = {}
    runs_failed = {}
    step_function_name = get_step_function_name(connection)
    if start_missing:
        targets.extend(check_result.get("files_without_md5run", []))
    if start_not_switched:
        targets.extend(check_result.get("files_with_run_and_wrong_status", []))
    action.output["targets"] = targets
    md5_workflow_uuid, md5_workflow_version = get_md5_workflow(connection)
    if md5_workflow_uuid:
        action.output["md5_workflow_uuid"] = md5_workflow_uuid
        action.output["md5_workflow_version"] = md5_workflow_version
    else:
        msg = "Unable to identify suitable MD5 Workflow on this environment"
        action.output["error"] = msg
        action.description = msg
        return action
    for target_file in targets:
        if is_past_time_limit(start, LAMBDA_LIMIT):
            action.description = "Did not complete action due to time limitations"
            break
        target_file_properties = ff_utils.get_metadata(
            target_file, key=connection.ff_keys, add_on="frame=raw"
        )
        workflow_run_common_fields = {
            "project": target_file_properties["project"],
            "institution": target_file_properties["institution"],
        }
        workflow_run_template = {
            "app_name": "md5",
            "workflow_uuid": md5_workflow_uuid,
            "config": {
                "ebs_size": 10,
                "instance_type": "t3.small",
                "EBS_optimized": True,
                "public_postrun_json": True,
                "behavior_on_capacity_limit": "wait_and_retry",
            },
            "common_fields": workflow_run_common_fields,
            "parameters": {},
            "custom_qc_fields": {},
        }
        file_parameters = {
            "input_file": target_file_properties["uuid"],
            "additional_file_parameters": {"input_file": {"mount": True}},
        }
        run_result = wfr_utils.run_missing_wfr(
            workflow_run_template,
            file_parameters,
            target_file_properties["accession"],
            connection.ff_keys,
            connection.ff_env,
            step_function_name,
        )
        if run_result.startswith("http"):  # Success is AWS URL
            runs_started[target_file] = run_result
        else:  # Failure is error message
            runs_failed[target_file] = run_result
    action.output["runs_started"] = runs_started
    action.output["runs_failed"] = runs_failed
    if not runs_failed:
        action.status = constants.ACTION_PASS
    return action


def get_md5_workflow(connection):
    """Get up-to-date MD5 workflow on the environment.

    MD5 workflows are expected to have explicit name of "md5" and to
    possess a version of the form x.x.x (e.g. v12.4.3) or x (e.g. v31),
    with the former considered the "correct", default form and the
    latter considered a fall-back.
    """
    md5_uuid = ""
    md5_version = ""
    three_version_md5s = {}
    single_version_md5s = {}
    three_version_pattern = re.compile(r"^(\d+)(\.(\d+)){2}$")
    single_version_pattern = re.compile(r"^(\d+)$")
    query = "/search/?type=Workflow&app_name=md5&field=uuid&field=app_version"
    search_results = ff_utils.search_metadata(query, key=connection.ff_keys)
    for md5_workflow in search_results:
        workflow_uuid = md5_workflow.get("uuid")
        version = md5_workflow.get("app_version", "").lstrip("v")
        if three_version_pattern.match(version):
            three_version_md5s[workflow_uuid] = version
        elif single_version_pattern.match(version):
            single_version_md5s[workflow_uuid] = version
    if three_version_md5s:
        for workflow_uuid, workflow_version in three_version_md5s.items():
            if workflow_version > md5_version:
                md5_version = workflow_version
                md5_uuid = workflow_uuid
    elif single_version_md5s:
        for workflow_uuid, workflow_version in single_version_md5s.items():
            if workflow_version > md5_version:
                md5_version = workflow_version
                md5_uuid = workflow_uuid
    return md5_uuid, md5_version


@check_function()
def metawfrs_to_run(connection, **kwargs):
    """Find MetaWorkflowRuns that may have WorkflowRuns to kick."""
    check = initialize_check("metawfrs_to_run", connection)
    check.action = "run_metawfrs"
    check.description = "Find MetaWorkflowRuns that have WorkflowRuns to kick."

    meta_workflow_runs = MetaWorkflowRunsFound(connection)
    meta_workflow_runs.search_final_status(FINAL_STATUS_TO_RUN)
    msg = "%s MetaWorkflowRun(s) may have WorkflowRuns to kick" % len(
        meta_workflow_runs.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_runs.uuids,
        "titles": meta_workflow_runs.titles,
    }
    if not meta_workflow_runs.uuids:
        check.allow_action = False
        check.status = constants.CHECK_PASS
    return check


@action_function()
def run_metawfrs(connection, **kwargs):
    """Kick WorkflowRuns on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action("run_metawfrs", connection, kwargs)
    action.description = "Start WorkflowRuns for MetaWorkflowRuns"

    success = []
    error = {}
    env = connection.ff_env
    step_function_name = get_step_function_name(connection)
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        if is_past_time_limit(start, LAMBDA_LIMIT):
            action.description = "Did not complete action due to time limitations"
            break
        try:
            run_metawfr.run_metawfr(
                meta_workflow_run_uuid,
                connection.ff_keys,
                sfn=step_function_name,
                env=env,
                valid_status=FINAL_STATUS_TO_RUN,
            )
            success.append(meta_workflow_run_uuid)
        except Exception as e:
            error[meta_workflow_run_uuid] = str(e)
    action.output["success"] = success
    action.output["error"] = error
    if not error:
        action.status = constants.ACTION_PASS
    return action


@check_function()
def metawfrs_to_checkstatus(connection, **kwargs):
    """Find MetaWorkflowRuns that may require a status check."""
    check = initialize_check("metawfrs_to_checkstatus", connection)
    check.action = "checkstatus_metawfrs"
    check.description = "Find MetaWorkflowRuns with WorkflowRuns to status check."

    meta_workflow_runs = MetaWorkflowRunsFound(connection)
    meta_workflow_runs.search_final_status(FINAL_STATUS_TO_CHECK)
    msg = "%s MetaWorkflowRun(s) may have WorkflowRuns to status check" % len(
        meta_workflow_runs.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_runs.uuids,
        "titles": meta_workflow_runs.titles,
    }
    if not meta_workflow_runs.uuids:
        check.allow_action = False
        check.status = constants.CHECK_PASS
    return check


@action_function()
def checkstatus_metawfrs(connection, **kwargs):
    """Check WorkflowRuns' status on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action("checkstatus_metawfrs", connection, kwargs)
    action.description = "Update WorkflowRuns' status on MetaWorkflowRuns"

    success = []
    error = {}
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        if is_past_time_limit(start, LAMBDA_LIMIT):
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
    if not error:
        action.status = constants.ACTION_PASS
    return action


@check_function()
def spot_failed_metawfrs(connection, **kwargs):
    """Find MetaWorkflowRuns with failed WorkflowRuns from spot
    interruptions.
    """
    check = initialize_check("spot_failed_metawfrs", connection)
    check.action = "reset_spot_failed_metawfrs"
    check.description = (
        "Find MetaWorkflowRuns with failed WorkflowRuns to reset spot failures"
    )

    meta_workflow_runs = MetaWorkflowRunsFound(connection)
    meta_workflow_runs.search_final_status(FINAL_STATUS_TO_RESET)
    msg = "%s MetaWorkflowRun(s) may have spot-failed WorkflowRuns to reset" % len(
        meta_workflow_runs.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_runs.uuids,
        "titles": meta_workflow_runs.titles,
    }
    if not meta_workflow_runs.uuids:
        check.allow_action = False
        check.status = constants.CHECK_PASS
    return check


@action_function()
def reset_spot_failed_metawfrs(connection, **kwargs):
    """Reset spot-failed WorkflowRuns on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action(
        "reset_spot_failed_metawfrs", connection, kwargs
    )
    action.description = "Reset spot-failed WorkflowRuns on MetaWorkflowRuns"

    success = {}
    error = {}
    s3_utils = s3Utils(env=connection.ff_env)
    log_bucket = s3_utils.tibanna_output_bucket
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        if is_past_time_limit(start, LAMBDA_LIMIT):
            action.description = "Did not complete action due to time limitations"
            break
        try:
            shards_to_reset = []
            meta_workflow_run = ff_utils.get_metadata(
                meta_workflow_run_uuid,
                add_on="frame=raw&datastore=database",
                key=connection.ff_keys,
            )
            meta_workflow_run_status = meta_workflow_run.get("status")
            if meta_workflow_run_status in ["deleted", "obsolete"]:
                continue
            workflow_runs = meta_workflow_run.get("workflow_runs", [])
            for workflow_run in workflow_runs:
                workflow_run_status = workflow_run.get("status")
                workflow_run_jobid = workflow_run.get("jobid")
                workflow_run_shard = workflow_run.get("shard")
                workflow_run_name = workflow_run.get("name")
                if workflow_run_status == "failed":
                    query = (
                        "/search/?type=WorkflowRunAwsem&field=description"
                        "&awsem_job_id=%s" % workflow_run_jobid
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
    if not error:
        action.status = constants.ACTION_PASS
    return action


@check_function(meta_workflow_runs=None)
def failed_metawfrs(connection, meta_workflow_runs=None, **kwargs):
    """Find failed MetaWorkflowRuns and reset failed WorkflowRuns."""
    check = initialize_check("failed_metawfrs", connection)
    check.action = "reset_failed_metawfrs"
    check.description = "Find failed MetaWorkflowRuns to reset all failed WorkflowRuns."

    meta_workflow_runs_found = MetaWorkflowRunsFound(connection)
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        meta_workflow_runs_found.add_items(found)
        meta_workflow_runs_not_found += not_found
    else:
        meta_workflow_runs_found.search_final_status(FINAL_STATUS_TO_RESET)
    msg = "%s MetaWorkflowRun(s) have failed WorkflowRuns to reset" % len(
        meta_workflow_runs_found.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = {
        "uuids": meta_workflow_runs_found.uuids,
        "titles": meta_workflow_runs_found.titles,
    }
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRun identifiers could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    if not meta_workflow_runs_found.uuids:
        check.allow_action = False
        if not meta_workflow_runs_not_found:
            check.status = constants.CHECK_PASS
    return check


@action_function()
def reset_failed_metawfrs(connection, **kwargs):
    """Reset all failed WorkflowRuns on MetaWorkflowRuns."""
    start = datetime.utcnow()
    action, check_result = initialize_action(
        "reset_failed_metawfrs", connection, kwargs
    )
    action.description = "Reset all failed WorkflowRuns on MetaWorkflowRuns"

    success = []
    error = {}
    meta_workflow_runs = check_result.get("meta_workflow_runs", {})
    meta_workflow_run_uuids = meta_workflow_runs.get("uuids", [])
    random.shuffle(meta_workflow_run_uuids)  # Ensure later ones hit within time limits
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        if is_past_time_limit(start, LAMBDA_LIMIT):
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
    if not error:
        action.status = constants.ACTION_PASS
    return action


@check_function(start_date=None, file_accessions=None)
def ingest_vcf_status(connection, start_date=None, file_accessions=None, **kwargs):
    """Search for full annotated VCF files that need to be ingested.

    kwargs:
        start_date -- limit search to files generated since a date,
            formatted YYYY-MM-DD
        file_accession -- run check with given files instead of the
            default query (expects comma/space separated accessions)
    """
    check = initialize_check("ingest_vcf_status", connection)
    check.action = "ingest_vcf_start"
    check.description = "Find VCFs to ingest"

    vcfs_to_ingest_uuids = []
    vcfs_to_ingest_accessions = []
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=False)
    if indexing_queue:
        msg = "Waiting for indexing queue to clear"
        check.brief_output.append(msg)
        check.summary = msg
        check.allow_action = False
        check.status = constants.CHECK_PASS
        return check
    old_style_query = (
        "/search/?file_type=full+annotated+VCF&type=FileProcessed"
        "&file_ingestion_status=No value&file_ingestion_status=N/A"
        "&status!=uploading&status!=to be uploaded by workflow&status!=upload failed"
    )
    new_style_query = (
        "/search/?vcf_to_ingest=true&type=FileProcessed"
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
        for result in search_results:
            vcfs_to_ingest_uuids.append(result.get("uuid"))
            vcfs_to_ingest_accessions.append(result.get("accession"))
    msg = "{} file(s) will be added to the ingestion queue".format(
        str(len(vcfs_to_ingest_uuids))
    )
    check.brief_output.append(msg)
    check.summary = msg
    check.full_output = {
        "files": vcfs_to_ingest_uuids,
        "accessions": vcfs_to_ingest_accessions,
    }
    if not vcfs_to_ingest_uuids:
        check.allow_action = False
        check.status = constants.CHECK_PASS
    return check


@action_function()
def ingest_vcf_start(connection, **kwargs):
    """POST VCF UUIDs to ingestion endpoint."""
    action, check_result = initialize_action("ingest_vcf_start", connection, kwargs)

    my_auth = connection.ff_keys
    targets = check_result["files"]
    post_body = {"uuids": targets}
    try:
        ff_utils.post_metadata(post_body, "/queue_ingestion", key=my_auth)
        action.output["queued for ingestion"] = targets
    except Exception as e:
        action.output["error"] = str(e)
    if action.output.get("error") is None:
        action.status = constants.ACTION_PASS
    return action


@check_function(file_accessions=None)
def check_vcf_ingestion_errors(connection, file_accessions=None, **kwargs):
    """
    Check for finding full annotated VCFs that have failed ingestion, so that they
    can be reset and the ingestion rerun if needed.
    """
    check = initialize_check("check_vcf_ingestion_errors", connection)
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
        first_ten_errors = []
        ingestion_errors = result.get("file_ingestion_error", [])
        # usually there are 100 errors, but just report first ten here
        for idx, error in enumerate(ingestion_errors):
            if idx == 10:
                break
            error_body = error.get("body")
            first_ten_errors.append(error_body)
        files_with_ingestion_errors[file_atid] = first_ten_errors
    msg = "%s File(s) found with ingestion errors" % len(search_response)
    check.brief_output.append(msg)
    check.summary = msg
    check.full_output = files_with_ingestion_errors
    if not files_with_ingestion_errors:
        check.status = constants.CHECK_PASS
        check.allow_action = False
    return check


@action_function()
def reset_vcf_ingestion_errors(connection, **kwargs):
    """Reset VCF metadata for reingestion."""
    action, check_result = initialize_action(
        "reset_vcf_ingestion_errors", connection, kwargs
    )

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
    if not error:
        action.status = constants.ACTION_PASS
    return action


@check_function()
def find_meta_workflow_runs_requiring_output_linktos(connection, **kwargs):
    """Find completed MetaWorkflowRuns to PATCH output files to desired
    locations.
    """
    check = initialize_check(
        "find_meta_workflow_runs_requiring_output_linktos", connection
    )
    check.description = "Find completed MetaWorkflowRuns to PATCH their output files."
    check.action = "link_meta_workflow_run_output_files"

    meta_workflow_runs = MetaWorkflowRunsFound(connection)
    query = (
        "search/?type=MetaWorkflowRun&final_status=completed&field=uuid"
        "&output_files_linked_status=No+value"
    )
    meta_workflow_runs.search_query(query)
    msg = "%s MetaworkflowRun(s) found to PATCH output files" % len(
        meta_workflow_runs.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = meta_workflow_runs.uuids
    if not meta_workflow_runs.uuids:
        check.allow_action = False
        check.status = constants.CHECK_PASS
    return check


@action_function()
def link_meta_workflow_run_output_files(connection, **kwargs):
    """PATCH MetaWorkflowRuns' designated output files to desired
    locations.
    """
    action, check_result = initialize_action(
        "link_meta_workflow_run_output_files", connection, kwargs
    )
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
    if not error:
        action.status = constants.ACTION_PASS
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
    check = initialize_check("find_meta_workflow_runs_with_linkto_errors", connection)
    check.action = "link_meta_workflow_run_output_files_after_error"
    check.description = "Find MetaWorkflowRuns with errors creating output file linkTos"

    meta_workflow_runs_found = MetaWorkflowRunsFound(connection)
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        link_status_error = []
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        for meta_workflow_run in found:
            linked_status = meta_workflow_run.get("output_files_linked_status")
            if linked_status == "error":
                link_status_error.append(meta_workflow_run)
        meta_workflow_runs_found.add_items(link_status_error)
        meta_workflow_runs_not_found += not_found
    else:
        query = (
            "search/?type=MetaWorkflowRun&field=uuid&output_files_linked_status=error"
        )
        meta_workflow_runs_found.search_query(query)
    msg = "%s MetaWorkflowRun(s) found with errors for output file links" % len(
        meta_workflow_runs_found.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = meta_workflow_runs_found.uuids
    if not meta_workflow_runs_found.uuids:
        check.allow_action = False
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRun(s) could not be found" % len(
            meta_workflow_runs_not_found
        )
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    if not meta_workflow_runs_found.uuids and not meta_workflow_runs_not_found:
        check.status = constants.CHECK_PASS
    return check


@action_function()
def link_meta_workflow_run_output_files_after_error(connection, **kwargs):
    """Attempt to PATCH output files to desired locations on a
    MetaWorkflowRun with prior errors.
    """
    action, check_result = initialize_action(
        "link_meta_workflow_run_output_files_after_error", connection, kwargs
    )
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
    if not error:
        action.status = constants.ACTION_PASS
    return action


@check_function(meta_workflow_runs=None, meta_workflows=None)
def find_meta_workflow_runs_to_kill(
    connection, meta_workflow_runs=None, meta_workflows=None, **kwargs
):
    """Find MetaWorkflowRuns to stop (won't be picked up by other
    MetaWorkflowRun checks/actions).
    """
    check = initialize_check("find_meta_workflow_runs_to_kill", connection)
    check.description = "Find MetaWorkflowRuns to stop further checks/actions"
    check.action = "kill_meta_workflow_runs"

    meta_workflow_runs_to_kill = MetaWorkflowRunsFound(connection)
    meta_workflow_runs_not_found = []
    if meta_workflow_runs is not None:
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        meta_workflow_runs_to_kill.add_items(found)
        meta_workflow_runs_not_found += not_found
    if meta_workflows is not None:
        meta_workflows = format_kwarg_list(meta_workflows)
        found, not_found = validate_items_existence(meta_workflows, connection)
        for meta_workflow in found:
            meta_workflow_uuid = meta_workflow.get("uuid")
            query = (
                "search/?type=MetaWorkflowRun&field=uuid&meta_workflow.uuid="
                + meta_workflow_uuid
                + "".join(
                    ["&final_status=" + status for status in FINAL_STATUS_TO_KILL]
                )
            )
            meta_workflow_runs_to_kill.search_query(query)
    if meta_workflows is None and meta_workflow_runs is None:
        meta_workflow_runs_to_kill.search_final_status(FINAL_STATUS_TO_KILL)
    uuids_to_kill = list(set(meta_workflow_runs_to_kill.uuids))
    msg = "%s MetaWorkflowRun(s) found to stop" % len(uuids_to_kill)
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["meta_workflow_runs"] = uuids_to_kill
    if not uuids_to_kill:
        check.allow_action = False
    if meta_workflow_runs_not_found:
        msg = "%s MetaWorkflowRuns were not found" % len(meta_workflow_runs_not_found)
        check.brief_output.append(msg)
        check.full_output["not_found"] = meta_workflow_runs_not_found
    if not uuids_to_kill and not meta_workflow_runs_not_found:
        check.status = constants.CHECK_PASS
    return check


@action_function()
def kill_meta_workflow_runs(connection, **kwargs):
    """Stop MetaWorkflowRuns from further foursight checks/actions."""
    action, check_result = initialize_action(
        "kill_meta_workflow_runs", connection, kwargs
    )
    action.description = "Stop MetaWorkflowfuns from further updates"

    success = []
    error = {}
    meta_workflow_runs_to_patch = check_result["meta_workflow_runs"]
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
    if not error:
        action.status = constants.ACTION_PASS
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
    check = initialize_check("find_sample_processing_for_meta_workflow", connection)
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
            meta_workflow_uuid = meta_workflow_properties.get("uuid")
            msg = "MetaWorkflow found: %s" % meta_workflow_name
            check.brief_output.append(msg)
            check.full_output["meta_workflow"] = meta_workflow_uuid
        else:
            msg = "MetaWorkflow not found: %s" % meta_workflow
            check.brief_output.append(msg)
    if cases:
        cases_found = []
        cases_without_sample_processing = []
        cases = format_kwarg_list(cases)
        found, cases_not_found = validate_items_existence(cases, connection)
        for case in found:
            sample_processing = case.get("sample_processing")
            case_uuid = case.get("uuid")
            cases_found.append(case_uuid)
            if sample_processing:
                sample_processings_for_meta_workflow.append(sample_processing)
            else:
                cases_without_sample_processing.append(case_uuid)
        msg = "%s Case(s) found" % len(cases_found)
        check.brief_output.append(msg)
        check.full_output["cases_found"] = cases_found
        if cases_not_found:
            msg = "%s Case(s) not found" % len(cases_not_found)
            check.brief_output.append(msg)
            check.full_output["cases_not_found"] = cases_not_found
        if cases_without_sample_processing:
            msg = "%s Case(s) lacked a SampleProcessing" % len(
                cases_without_sample_processing
            )
            check.brief_output.append(msg)
            check.full_output[
                "cases_without_sample_processing"
            ] = cases_without_sample_processing
    if sample_processings:
        sample_processings_found = []
        sample_processings = format_kwarg_list(sample_processings)
        found, not_found = validate_items_existence(sample_processings, connection)
        for sample_processing in found:
            sample_processings_found.append(sample_processing.get("uuid"))
        sample_processings_for_meta_workflow += sample_processings_found
        msg = "%s SampleProcessing(s) were found" % len(sample_processings_found)
        check.brief_output.append(msg)
        check.full_output["sample_processings_found"] = sample_processings_found
        if not_found:
            msg = "%s SampleProcessing(s) not found" % len(not_found)
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
        check.brief_output.append(msg)
        check.summary = msg
        check.allow_action = False
    return check


@action_function()
def create_meta_workflow_runs_for_items(connection, **kwargs):
    """Create MetaWorkflowRuns."""
    action, check_result = initialize_action(
        "create_meta_workflow_runs_for_items", connection, kwargs
    )
    action.description = "Create MetaWorkflowRuns"

    sample_success = []
    sample_error = {}
    sample_processing_success = []
    sample_processing_error = {}
    meta_workflow_identifier = check_result.get("meta_workflow")
    sample_processings = check_result.get("sample_processing_for_meta_workflow", [])
    samples = check_result.get("samples_for_meta_workflow", [])
    if sample_processings:
        for sample_processing in sample_processings:
            try:
                MetaWorkflowRunFromSampleProcessing(
                    sample_processing, meta_workflow_identifier, connection.ff_keys
                ).post_and_patch()
                sample_processing_success.append(sample_processing)
            except Exception as error_msg:
                sample_processing_error[sample_processing] = str(error_msg)
        action.output[
            "sample_processings_with_meta_workflow_runs"
        ] = sample_processing_success
        action.output["sample_processings_with_errors"] = sample_processing_error
    if samples:
        for sample in samples:
            try:
                MetaWorkflowRunFromSample(
                    sample, meta_workflow_identifier, connection.ff_keys
                ).post_and_patch()
                sample_success.append(sample)
            except Exception as error_msg:
                sample_error[sample] = str(error_msg)
        action.output["samples_with_meta_workflow_runs"] = sample_success
        action.output["samples_with_errors"] = sample_error
    if not sample_error and not sample_processing_error:
        action.status = constants.ACTION_PASS
    return action


@check_function(
    meta_workflow="",
    cases=None,
    sample_processings=None,
    samples=None,
)
def find_sample_for_meta_workflow(
    connection,
    meta_workflow="",
    cases=None,
    sample_processings=None,
    samples=None,
    **kwargs,
):
    """Find Sample(s) and MetaWorkflow to create new
    MetaWorkflowRun(s).
    """
    check = initialize_check("find_sample_for_meta_workflow", connection)
    check.description = "Find Samples and MetaWorkflow to create new MetaWorkflowRuns"
    check.action = "create_meta_workflow_runs_for_items"

    samples_for_meta_workflow = set()
    sample_processings_from_cases = set()
    samples_from_sample_processings = set()
    if meta_workflow:
        meta_workflow_found, _ = validate_items_existence(meta_workflow, connection)
        if meta_workflow_found:
            meta_workflow_properties = meta_workflow_found[0]  # Only 1 MWF expected
            meta_workflow_name = meta_workflow_properties.get("name")
            meta_workflow_uuid = meta_workflow_properties.get("uuid")
            msg = "MetaWorkflow found: %s" % meta_workflow_name
            check.brief_output.append(msg)
            check.full_output["meta_workflow"] = meta_workflow_uuid
        else:
            msg = "MetaWorkflow not found: %s" % meta_workflow
            check.brief_output.append(msg)
    if cases:
        cases_found = []
        cases_without_sample_processing = []
        cases = format_kwarg_list(cases)
        found, cases_not_found = validate_items_existence(cases, connection)
        for case in found:
            sample_processing = case.get("sample_processing")
            case_uuid = case.get("uuid")
            cases_found.append(case_uuid)
            if sample_processing:
                sample_processings_from_cases.add(sample_processing)
            else:
                cases_without_sample_processing.append(case_uuid)
        msg = "%s Case(s) found" % len(cases_found)
        check.brief_output.append(msg)
        check.full_output["cases_found"] = cases_found
        if cases_not_found:
            msg = "%s Case(s) not found" % len(cases_not_found)
            check.brief_output.append(msg)
            check.full_output["cases_not_found"] = cases_not_found
        if cases_without_sample_processing:
            msg = "%s Case(s) lacked a SampleProcessing" % len(
                cases_without_sample_processing
            )
            check.brief_output.append(msg)
            check.full_output[
                "cases_without_sample_processing"
            ] = cases_without_sample_processing
    if sample_processings or sample_processings_from_cases:
        sample_processings_found = []
        sample_processings = format_kwarg_list(sample_processings)
        sample_processings |= sample_processings_from_cases
        found, not_found = validate_items_existence(sample_processings, connection)
        for sample_processing in found:
            sample_processings_found.append(sample_processing.get("uuid"))
            for sample in sample_processing.get("samples", []):
                samples_from_sample_processings.add(sample)
        msg = "%s SampleProcessing(s) were found" % len(sample_processings_found)
        check.brief_output.append(msg)
        check.full_output["sample_processings_found"] = sample_processings_found
        if not_found:
            msg = "%s SampleProcessing(s) not found" % len(not_found)
            check.brief_output.append(msg)
            check.full_output["sample_processings_not_found"] = not_found
    if samples or samples_from_sample_processings:
        samples = format_kwarg_list(samples)
        samples |= samples_from_sample_processings
        found, not_found = validate_items_existence(samples, connection)
        for sample in found:
            samples_for_meta_workflow.add(sample.get("uuid"))
        if not_found:
            msg = "%s Sample(s) not found" % len(not_found)
            check.brief_output.append(msg)
            check.full_output["samples_not_found"] = not_found
    msg = "%s Sample(s) found to use for MetaWorkflowRun creation" % len(
        samples_for_meta_workflow
    )
    check.brief_output.append(msg)
    check.full_output["samples_for_meta_workflow"] = list(samples_for_meta_workflow)
    if samples_for_meta_workflow and meta_workflow:
        msg = "Action will create %s MetaWorkflowRun(s) for MetaWorkflow %s" % (
            len(samples_for_meta_workflow),
            meta_workflow,
        )
        check.brief_output.append(msg)
        check.summary = msg
    else:
        msg = "Could not find information required to create MetaWorkflowRuns"
        check.brief_output.append(msg)
        check.summary = msg
        check.allow_action = False
    return check


@check_function(meta_workflow_runs=None)
def find_meta_workflow_runs_with_quality_metric_failure(
    connection, meta_workflow_runs=None, **kwargs
):
    """Find MetaWorkflowRuns with output QualityMetric failure(s)."""
    check = initialize_check(
        "find_meta_workflow_runs_with_quality_metric_failure", connection
    )
    check.action = "ignore_quality_metric_failure_for_meta_workflow_run"
    check.description = "Find MetaWorkflowRuns with output QualityMetric failure(s)"

    meta_workflow_runs_found = MetaWorkflowRunsFound(connection)
    meta_workflow_runs_not_found = []
    if meta_workflow_runs:
        quality_metric_failed = []
        meta_workflow_runs = format_kwarg_list(meta_workflow_runs)
        found, not_found = validate_items_existence(meta_workflow_runs, connection)
        for meta_workflow_run in found:
            final_status = meta_workflow_run.get("final_status")
            if final_status == "quality metric failed":
                quality_metric_failed.append(meta_workflow_run)
        meta_workflow_runs_found.add_items(quality_metric_failed)
        meta_workflow_runs_not_found += not_found
    else:
        query = (
            "search/?type=MetaWorkflowRun&field=uuid&final_status=quality+metric+failed"
        )
        meta_workflow_runs_found.search_query(query)
    msg = "%s MetaWorkflowRun(s) found with failed output QualityMetrics" % len(
        meta_workflow_runs_found.uuids
    )
    check.summary = msg
    check.brief_output.append(msg)
    check.full_output["failing_quality_metrics"] = meta_workflow_runs_found.uuids
    if not meta_workflow_runs_found.uuids:
        check.status = constants.CHECK_PASS
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
    action, check_result = initialize_action(
        "ignore_quality_metric_failure_for_meta_workflow_run", connection, kwargs
    )
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
    if not error:
        action.status = constants.ACTION_PASS
    return action
