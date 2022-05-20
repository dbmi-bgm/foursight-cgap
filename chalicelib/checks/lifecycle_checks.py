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


@check_function(files_per_run=100, first_check_after=14, max_checking_frequency=14)
def check_file_lifecycle_status(connection, **kwargs):
    """
    Inspect and find files whose lifecycle status need patching.
    Additional argumements:
    files_per_run (int): determines how many files to check at once. Default: 20
    first_check_after (int): number of days after upload of a file, when lifecycle status starts to be checked
    max_checking_frequency (int): determines how often a file is checked at most (in days). Default 14 (days).
    """

    check = CheckResult(connection, "check_file_lifecycle_status")
    my_auth = connection.ff_keys
    check.action = "patch_file_lifecycle_status"
    check.description = (
        "Inspect and find files whose lifecycle status need patching"
    )
    check.summary = ""
    check.full_output = {}
    check.status = "PASS"

    num_files_to_check = kwargs.get("files_per_run", 100)
    first_check_after = kwargs.get("first_check_after", 14)
    max_checking_frequency = kwargs.get("max_checking_frequency", 14)

    # We only want to get files from the portal that have a lifecycle category set and have either never been checked
    # or previously checked sufficiently long ago - as far as I know this can't be combined into one query. Furthermore,
    # they should be at least {first_check_after} days old
    threshold_date_fca = datetime.date.today() - datetime.timedelta(first_check_after)
    threshold_date_fca = threshold_date_fca.strftime("%Y-%m-%d")
    threshold_date_mcf = datetime.date.today() - datetime.timedelta(max_checking_frequency)
    threshold_date_mcf = threshold_date_mcf.strftime("%Y-%m-%d")

    search_query_base = (
        "/search/?type=FileProcessed&type=FileFastq"
        "&s3_lifecycle_category%21=No+value"
        f"&date_created.to={threshold_date_fca}"
        "&status=uploaded"
        "&status=archived"
        "&status=shared"
        f"&limit={num_files_to_check // 2}"
        )
    search_query_1 = f"{search_query_base}&s3_lifecycle_last_checked.to={threshold_date_mcf}"
    search_query_2 = f"{search_query_base}&s3_lifecycle_last_checked=No+value"

    all_files = ff_utils.search_metadata(search_query_1, key=my_auth)
    all_files = all_files + ff_utils.search_metadata(search_query_2, key=my_auth)
        
    # This will contain the files that require lifecycle updates  
    files_to_update = []
    files_not_being_checked = []
    logs = []

    # This dict will contain all the lifecycle policies per project that are relevant
    # for the current set of files. "project.lifecycle_policy" is not embedded in the File
    # item and we want to retrieve the project metadata only once for each project.
    lifecycle_policies_by_project = {}

    for file in all_files:
        file_uuid = file['uuid']
        print(file_uuid)

        if file["s3_lifecycle_category"] == lifecycle_utils.IGNORE:
            files_not_being_checked.append(file_uuid)
            continue

        # Get the correct lifecylce policy - load it from the metadata only once
        project_uuid = file["project"]["uuid"]
        if project_uuid not in lifecycle_policies_by_project:
            project = ff_utils.get_metadata(project_uuid, key=my_auth)
            if "lifecycle_policy" in project:
                lifecycle_policies_by_project[project_uuid] = project["lifecycle_policy"]
            else:
                lifecycle_policies_by_project[project_uuid] = lifecycle_utils.default_lifecycle_policy

        lifecycle_policy = lifecycle_policies_by_project[project_uuid]

        file_lifecycle_category = file["s3_lifecycle_category"] # e.g. "long_term_archive"
        if file_lifecycle_category not in lifecycle_policy:
            check.status = "WARN"
            check.warning = "Some files have unknown lifecycle categories. Check logs."
            logs.append(f'File {file_uuid} has an unknown lifecycle category {file_lifecycle_category}')
            continue

        # This contains the applicable rules for the current file, e.g., {MOVE_TO_DEEP_ARCHIVE_AFTER: 0, EXPIRE_AFTER: 12}
        file_lifecycle_policy = lifecycle_policy[file_lifecycle_category]

        file_old_lifecycle_status = file["s3_lifecycle_status"]
        file_new_lifecycle_status = lifecycle_utils.get_file_lifecycle_status(file, file_lifecycle_policy)

        #Check that the new storage class is indeed "deeper" than the old one. We can't transfer files to more accessible storage classes
        file_old_lifecycle_status_int = lifecycle_utils.lifecycle_status_to_int(file_old_lifecycle_status)
        file_new_lifecycle_status_int = lifecycle_utils.lifecycle_status_to_int(file_new_lifecycle_status)
        if(file_old_lifecycle_status_int > file_new_lifecycle_status_int):
            check.status = "WARN"
            check.warning = "Unsupported storage class transition for some file. Check logs"
            logs.append(f'File {file_uuid} wants to transition from {file_old_lifecycle_status} to {file_new_lifecycle_status}')
            continue

        if file_old_lifecycle_status != file_new_lifecycle_status:
            update_dict = {
                    "uuid": file_uuid,
                    "upload_key": file["upload_key"],
                    "old_lifecycle_status": file_old_lifecycle_status,
                    "new_lifecycle_status": file_new_lifecycle_status,
                    "is_extra_file": False
                }
            files_to_update.append(update_dict)

            # Get extra files and update those as well. They will be treated like the original file
            extra_files = file.get("extra_files", [])
            for ef in extra_files:
                ef_update_dict = update_dict.copy()
                ef_update_dict["upload_key"] = ef["upload_key"]
                ef_update_dict["is_extra_file"] = True
                files_to_update.append(ef_update_dict)


    check.summary = f'{len(files_to_update)} files require patching.'

    check.full_output = {
        "files_to_update": files_to_update,
        "files_not_being_checked": files_not_being_checked,
        "logs": logs
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
    action_logs['patched_files'] = []
    action_logs['error'] = []

    files = check_output.get('files_to_update', [])

    for file in files:
        uuid = file["uuid"]
        upload_key = file["upload_key"]
        old_lifecycle_status = file["old_lifecycle_status"]
        new_lifecycle_status = file["new_lifecycle_status"]
        is_extra_file = file["is_extra_file"]

        # Before tagging the file, we need to verify that it actually exists on S3. However, the correct
        # bucket cannot be easily inferred from the file meta data currently. Most files will be
        # in the out_bucket.
        file_bucket = None 
        if my_s3_util.does_key_exist(upload_key, bucket=out_bucket, print_error=False):
            file_bucket = out_bucket
        elif my_s3_util.does_key_exist(upload_key, bucket=raw_bucket, print_error=False):
            file_bucket = raw_bucket
        if not file_bucket:
            action_logs['error'].append(f'Cannot patch file {uuid}: not found on S3')
            continue

        try:
            if not is_extra_file:
                today = datetime.date.today().strftime("%Y-%m-%d")
                file_status = lifecycle_utils.lifecycle_status_to_file_status[new_lifecycle_status]
                patch_dict = {
                    's3_lifecycle_status': new_lifecycle_status,
                    's3_lifecycle_last_checked': today,
                    'status': file_status
                }
                ff_utils.patch_metadata(patch_dict, uuid, key=my_auth)
            s3_tag = lifecycle_utils.lifecycle_status_to_s3_tag(new_lifecycle_status)
            if s3_tag:
                my_s3_util.set_object_tags(upload_key, file_bucket, s3_tag, replace_tags=True)
            else: 
                raise Exception("Could not determine S3 tag")
            action_logs['patched_files'].append(f'Lifecycle status of file {uuid} changed from {old_lifecycle_status} to {new_lifecycle_status}')
        
        except Exception as e:
            action_logs['error'].append(f'Error patching or tagging file {uuid}: {str(e)}')
            continue

    action.output = action_logs
    # we want to display an error if there are any errors in the run, even if many patches are successful
    if action_logs['error'] == []:
        action.status = 'DONE'
    else:
        action.status = 'ERROR'
    return action

