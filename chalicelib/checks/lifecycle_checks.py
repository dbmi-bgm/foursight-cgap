import json
import random
import pprint
import datetime
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
    print("num_metawfrs_in_portal", num_metawfrs_in_portal)
    print("num_metawfrs_to_check", num_metawfrs_to_check)

    # Get {num_metawfrs_to_check} random MetaWorkflowRuns
    limit = str(num_metawfrs_to_check)
    search_from = str(random.randint(0, num_metawfrs_in_portal))
    search_from = str(10)
    search_metawfrs = (
        "/search/?type=MetaWorkflowRun" + "&limit=" + limit + "&from=" + search_from
    )
    result_metawfrs = ff_utils.search_metadata(search_metawfrs, key=my_auth)
    print(search_metawfrs)

    # This will contain the metawfr that have been processed and require meta data updates
    metawfrs_to_update = []
    files_without_lifecycle_category = []
    files_to_update = []

    for metawfr in result_metawfrs:

        if not lifecycle_utils.should_mwfr_be_checked(metawfr, max_checking_frequency):
            continue

        lifecycle_policy = lifecycle_utils.default_lifecycle_policy
        if "lifecycle_policy" in metawfr["project"]:
            lifecycle_policy = metawfr["project"]["lifecycle_policy"]

        # Information about the lifecycle categories of workflow files
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

        # The lifecycle status of the metawfr will be "pending" if there exists an associated file
        # that is not deleted or ignored
        metawfr_lifecycle_status = lifecycle_utils.COMPLETE

        for file in all_files:
            file_metadata = ff_utils.get_metadata(file["uuid"], key=my_auth)

            # This is a best guess of the lifecycle category.
            # Input files don't have their lifecycle category stored in the metadata,
            # therefore we have to rely on this function for those
            file_lifecycle_category = lifecycle_utils.get_lifecycle_category(file_metadata)

            # If we deal with a workflow output file, try to replace the lifecycle category with the one
            # attached to the corresponding metadata.
            wf_name = file.get("wf_name")
            if wf_name:
                file_lifecycle_category = wf_lc_map.get(wf_name)

            if not file_lifecycle_category:
                check.status = "WARN"
                check.warning = "Could not assign a lifecycle category to some files."
                files_without_lifecycle_category.append(file)
                metawfr_lifecycle_status = lifecycle_utils.PENDING
            else:
                old_file_lifecycle_status = file_metadata.get("s3_lifecycle_status")
                if old_file_lifecycle_status == lifecycle_utils.IGNORE:
                    continue
                
                file_lifecycle_policy = lifecycle_policy.get(file_lifecycle_category)
                new_file_lifecycle_status = lifecycle_utils.get_file_lifecycle_status(
                    file_metadata, file_lifecycle_policy
                )

                if new_file_lifecycle_status != lifecycle_utils.DELETED:
                    metawfr_lifecycle_status = lifecycle_utils.PENDING

                if old_file_lifecycle_status != new_file_lifecycle_status:
                    update_dict = {
                            "uuid": file_metadata["uuid"],
                            "upload_key": file_metadata["upload_key"],
                            "old_lifecycle_status": old_file_lifecycle_status,
                            "new_lifecycle_status": new_file_lifecycle_status,
                            "metawfr_uuid": metawfr["uuid"], 
                            "is_extra_file": False
                        }
                    files_to_update.append(update_dict)

                    # Get extra files and update those as well. They will be treated like the original file
                    extra_files = file_metadata.get("extra_files")
                    for ef in extra_files:
                        ef_update_dict = update_dict.copy()
                        ef_update_dict["upload_key"] = ef["upload_key"]
                        ef_update_dict["is_extra_file"] = True
                        files_to_update.append(ef_update_dict)
                        

        # Update the lifecycle status of the metawfr itself. It has status "pending" until 
        # all files associated with it have been deleted or can be ignored.
        now_es = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
        metawfrs_to_update.append(
            {
                "uuid": metawfr["uuid"],
                "new_lifecycle_status": metawfr_lifecycle_status,
                "last_checked": now_es
            }
        )

    check.summary = f'{len(files_to_update)} files in {len(metawfrs_to_update)} MetaWorkflowRuns require patching.'

    check.full_output = {
        "metawfrs_to_update": metawfrs_to_update,
        "files_without_lifecycle_category": files_without_lifecycle_category,
        "files_to_update": files_to_update,
    }

    return check



@action_function()
def patch_file_lifecycle_status(connection, **kwargs):
    # start = datetime.utcnow()
    action = ActionResult(connection, 'patch_file_lifecycle_status')
    my_auth = connection.ff_keys
    env = connection.ff_env
    my_s3_util = s3Utils(env=env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket
    check_result = action.get_associated_check_result(kwargs)
    check_output = check_result.get('full_output', {})
    action_logs = {}
    action_logs['check_output'] = check_output
    action_logs['patched_metawfrs'] = []
    action_logs['patched_files'] = []
    action_logs['error'] = []

    files = check_output.get('files_to_update', [])
    metawfrs = check_output.get('metawfrs_to_update', [])

    for metawfr_dict in metawfrs:
        metawfr_uuid = metawfr_dict["uuid"]
        metawfr_files = [f for f in files if f['metawfr_uuid'] == metawfr_uuid]
        
        for metawfr_file in metawfr_files:
            f_uuid = metawfr_file["uuid"]
            f_upload_key = metawfr_file["upload_key"]
            f_ols = metawfr_file["old_lifecycle_status"]
            f_nls = metawfr_file["new_lifecycle_status"]
            f_is_extra = metawfr_file["is_extra_file"]

            # Before tagging the file, we need to verify that it actually exists on S3. However, the correct
            # bucket cannot be easily inferred from the file meta data currently. Most files will be
            # in the out_bucket.
            f_bucket = None 
            if my_s3_util.does_key_exist(f_upload_key, bucket=out_bucket, print_error=False):
                f_bucket = out_bucket
            elif my_s3_util.does_key_exist(f_upload_key, bucket=raw_bucket, print_error=False):
                f_bucket = raw_bucket
            if not f_bucket:
                action_logs['error'].append(f'Cannot patch file {f_uuid}: not found on S3')
                continue

            try:
                if not f_is_extra:
                    ff_utils.patch_metadata({'s3_lifecycle_status': f_nls}, f_uuid, key=my_auth)
                s3_tag = lifecycle_utils.lifecycle_status_to_s3_tag(f_nls)
                if s3_tag:
                    my_s3_util.set_object_tags(f_upload_key, f_bucket, s3_tag, replace_tags=True)
                else: 
                    raise Exception("Could not determine S3 tag")
                action_logs['patched_files'].append(f'Lifecycle status of file {f_uuid} changed from {f_ols} to {f_nls}')
            
            except Exception as e:
                action_logs['error'].append(f'Error patching or tagging file {f_uuid}: {str(e)}')
                continue

        try:
            updated_mwfr_lifecycle_status = {
                "status": metawfr_dict["new_lifecycle_status"],
                "last_checked": metawfr_dict["last_checked"],
            }
            ff_utils.patch_metadata({'lifecycle_status': updated_mwfr_lifecycle_status}, metawfr_uuid, key=my_auth)
            action_logs['patched_metawfrs'].append(metawfr_uuid)

        except Exception as e:
            action_logs['error'].append(f'Error patching MetaWorkflowRun {metawfr_uuid}: {str(e)}')
            continue

    action.output = action_logs
    # we want to display an error if there are any errors in the run, even if many patches are successful
    if action_logs['error'] == []:
        action.status = 'DONE'
    else:
        action.status = 'ERROR'
    return action

