import json
import random
import pprint
from datetime import datetime
from dcicutils import ff_utils, s3Utils

from .helpers import lifecycle_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *

pp = pprint.PrettyPrinter(indent=2)


@check_function(metawfrs_per_run=5, max_checking_frequency=7)
def check_metawfrs_lifecycle_status(connection, **kwargs):
    """
    Inspect metaworkflow runs and find files whose lifecycle status needs patching.
    Additional argumements:
    metawfrs_per_run (int): determines how many metawfrs to check at once. Default: 5
    max_checking_frequency (int): determines how often a metawfr is checked at most (in days). Default 7 (days).
    """

    check = CheckResult(connection, "check_metawfrs_lifecycle_status")
    my_auth = connection.ff_keys
    check.action = "patch_file_lifecycle_status"
    check.description = (
        "Inspect metaworkflow runs and find files whose lifecycle status needs patching"
    )
    check.brief_output = {}
    check.summary = ""
    check.full_output = {}
    check.status = "PASS"

    # check indexing queue
    env = connection.ff_env

    counts_info = ff_utils.get_counts_page(ff_env=env)
    counts_info_metawfrs = counts_info["db_es_compare"]["MetaWorkflowRun"]
    num_metawfrs_in_portal = int(counts_info_metawfrs.split()[1])
    num_metawfrs_to_check = kwargs.get("metawfrs_per_run", 5)
    max_checking_frequency = kwargs.get("max_checking_frequency", 7)
    print(num_metawfrs_in_portal, num_metawfrs_to_check)

    # Get {num_metawfrs_to_check} random MetaWorkflowRuns
    limit = str(num_metawfrs_to_check)
    search_from = str(random.randint(0, num_metawfrs_in_portal))
    search_from = str(78)
    search_metawfrs = (
        "/search/?type=MetaWorkflowRun" + "&limit=" + limit + "&from=" + search_from
    )
    result_metawfrs = ff_utils.search_metadata(search_metawfrs, key=my_auth)
    print(search_metawfrs)

    # This will contain the metawfr that have been processed and require meta data updates
    metawfr_to_update = []
    files_without_lifecycle_category = []
    files_to_update = []
    files_to_update_brief = [] # This will be used for display in check/action output

    for metawfr in result_metawfrs:

        # TODO: Add stopped here as well?
        valid_final_status = ["completed"]  # Only these are checked in the following
        if metawfr["final_status"] not in valid_final_status:
            continue

        # If lifecycle_info is present, the metawfr was been checked before.
        # Make sure enough time passed to check it again
        # TODO Check the following, once the metatdat is there
        if "lifecycle_info" in metawfr:
            metawfr_lifecycle_info = metawfr["lifecycle_info"]
            now = datetime.datetime.utcnow()
            last_checked = metawfr_lifecycle_info["last_checked"]
            delta = now - last_checked
            # check this metawfr at most every {max_checking_frequency} days
            if delta.total_seconds() < max_checking_frequency * 24 * 60 * 60:
                continue

        if not lifecycle_utils.has_bam_qc_passed():
            continue

        if not lifecycle_utils.are_variants_ingested():
            continue

        metawfr_to_update.append(metawfr)

        lifecycle_policy = lifecycle_utils.default_lifecycle_policy
        if "lifecycle_policy" in metawfr["project"]:
            lifecycle_policy = metawfr["project"]["lifecycle_policy"]

        # Information about the lifecycle categories of the files
        # can be found in the meta workflow (custom_pf_field of each workflow)
        meta_workflow_uuid = metawfr["meta_workflow"]["uuid"]
        meta_workflow = ff_utils.get_metadata(meta_workflow_uuid, key=my_auth)

        # This dict contains information about the output file lifecycle category of a workflow
        wf_lc_map = lifecycle_utils.get_workflow_lifecycle_category_map(meta_workflow)

        # we are collecting all files that are connected to the metawfr and then check if they need updating
        all_files = []

        for input in metawfr["input"]:
            if input["argument_type"] != "file":
                continue
            for file in input["files"]:
                file_info = file["file"]
                all_files.append({
                    "uuid": file_info["uuid"]
                })

        for workflow in metawfr["workflow_runs"]:
            if "output" not in workflow:
                continue
            wf_name = workflow["name"]
            for file in workflow["output"]:
                if "file" not in file:
                    continue
                file_info = file["file"]
                all_files.append({
                    "uuid": file_info["uuid"],
                    "wf_name": wf_name
                })

        for file in all_files:
            file_metadata = ff_utils.get_metadata(file["uuid"], key=my_auth)

            # This is a best guess of the lifecycle category.
            # Input files don't have their lifecycle category stored in the metadata,
            # therefore we have to rely on this function for those
            lifecycle_category = lifecycle_utils.get_lifecycle_category(file_metadata)

            # If we deal with a workflow output file, try to replace the lifecycle category with the one
            # attached to the corresponding metadata.
            wf_name = file.get("wf_name")
            if wf_name and (wf_name in wf_lc_map):
                lifecycle_category = wf_lc_map[wf_name]

            if not lifecycle_category:
                check.status = "WARN"
                check.summary = (
                    "Could not assign a lifecycle category to some files."
                )
                check.description = check.summary
                files_without_lifecycle_category.append(file)
            else:
                old_lifecycle_status = file_metadata.get("lifecycle_status")
                new_lifecycle_status = lifecycle_utils.get_lifecycle_status(
                    file_metadata, lifecycle_policy.get(lifecycle_category)
                )

                if old_lifecycle_status != new_lifecycle_status:
                    files_to_update.append(
                        {
                            "file_metadata": file_metadata,
                            "new_lifecycle_status": new_lifecycle_status,
                            "metawfr_uuid": metawfr[
                                "uuid"
                            ], 
                        }
                    )
                    files_to_update_brief.append(
                        {
                            "uuid": file_metadata["uuid"],
                            "upload_key": file_metadata["upload_key"],
                            "old_lifecycle_status": old_lifecycle_status,
                            "new_lifecycle_status": new_lifecycle_status,
                            "metawfr_uuid": metawfr[
                                "uuid"
                            ], 
                        }
                    )
           
        #     pp.pprint(workflow)

        print(metawfr["uuid"], metawfr["final_status"])
        # pp.pprint(metawfr)
    if len(files_to_update) == 0:
        check.summary = "There are no files that need patching."

    check.full_output = {
        "metawfr_to_update": metawfr_to_update,
        "files_without_lifecycle_category": files_without_lifecycle_category,
        "files_to_update": files_to_update,
    }

    check.brief_output = {
        "metawfr_to_update": [d["uuid"] for d in metawfr_to_update],
        "files_without_lifecycle_category": files_without_lifecycle_category,
        "files_to_update": files_to_update_brief,
    }

    return check



@action_function()
def patch_file_lifecycle_status(connection, **kwargs):
    # start = datetime.utcnow()
    action = ActionResult(connection, 'patch_file_lifecycle_status')
    action_logs = {'metawfrs_updated': [], 'metawfrs_that_failed_linecount_test': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs)
    check_output = check_result.get('full_output', {})
    check_brief_output = check_result.get('brief_output', {})
    action_logs['check_output'] = check_brief_output
    action_logs['patched_metawfrs'] = []
    action_logs['patched_files'] = []
    action_logs['error'] = []

    files = check_output.get('files_to_update', [])
    metawfrs = check_output.get('metawfr_to_update', [])
    for metawfr in metawfrs:
        metawfr_uuid = metawfr["uuid"]
        metawfr_files = [f for f in files if f['metawfr_uuid'] == metawfr_uuid]
        action_logs['patched_metawfrs'].append(metawfr_uuid)
        for metawfr_file in metawfr_files:
            file_uuid = metawfr_file["file_metadata"]["uuid"]
            action_logs['patched_files'].append(file_uuid)

    # for metawfr_uuid in metawfr_uuids:
    #     now = datetime.utcnow()
    #     if (now-start).seconds > lambda_limit:
    #         action.description = 'Did not complete action due to time limitations'
    #         break
    #     try:
    #         metawfr_meta = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw', key=my_auth)
    #         # we have a few different dictionaries of steps to check output from in linecount_dicts.py
    #         # the proband-only and family workflows have the same steps, so we assign the proband_SNV_dict
    #         if 'Proband-only' in metawfr_meta['title'] or 'Family' in metawfr_meta['title']:
    #             steps_dict = proband_SNV_dict
    #         # trio has novoCaller, so it has a separate dictionary of steps
    #         elif 'Trio' in metawfr_meta['title']:
    #             steps_dict = trio_SNV_dict
    #         # cnv/sv is a completely different pipeline, so has a many different steps
    #         elif 'CNV' in metawfr_meta['title']:
    #             steps_dict = CNV_dict
    #         # if this is run on something other than those expected MWFRs, we want an error.
    #         else:
    #             e = 'Unexpected MWF Title: '+metawfr_meta['title']
    #             action_logs['error'].append(str(e))
    #             continue
    #         # this calls check_lines from cgap-pipeline pipeline_utils check_lines.py (might get moved to generic repo in the future)
    #         # will return TRUE or FALSE if all pipeline steps are fine, or if there are any that do not match linecount with their partners, respectively
    #         linecount_result = check_lines(metawfr_uuid, my_auth, steps=steps_dict, fastqs=fastqs_dict)
    #         #want an empty dictionary if no overall_qcs, or a dictionary of tests and results if there are items in the overall_qcs list
    #         overall_qcs_dict = {qc['name']: qc['value'] for qc in metawfr_meta.get('overall_qcs', [])}
    #         overall_qcs_dict['linecount_test'] = 'PASS' if linecount_result else 'FAIL'
    #         # turn the dictionary back into a list of dictionaries that is properly structured (e.g., overall_qcs: [{"name": "linecount_test", "value": "PASS"}, {...}, {...}])
    #         updated_overall_qcs = [{'name': k, 'value': v} for k, v in overall_qcs_dict.items()]
    #         try:
    #             ff_utils.patch_metadata({'overall_qcs': updated_overall_qcs}, metawfr_uuid, key=my_auth)
    #             if linecount_result:
    #                 action_logs['metawfrs_that_passed_linecount_test'].append(metawfr_uuid)
    #             else:
    #                 action_logs['metawfrs_that_failed_linecount_test'].append(metawfr_uuid)
    #         except Exception as e:
    #             action_logs['error'].append(str(e))
    #             continue
    #     except Exception as e:
    #         action_logs['error'].append(str(e))
    #         continue
    action.output = action_logs
    # we want to display an error if there are any errors in the run, even if many patches are successful
    if action_logs['error'] == []:
        action.status = 'DONE'
    else:
        action.status = 'ERROR'
    return action

