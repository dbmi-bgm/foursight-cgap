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


@check_function(files_per_run=100, first_check_after=14, max_checking_frequency=14)
def check_file_lifecycle_status(connection, **kwargs):
    """
    Inspect and find files whose lifecycle status need patching.
    Additional arguments:
    files_per_run (int): determines how many files to check at once. Default: 100
    first_check_after (int): number of days after upload of a file, when lifecycle status starts to be checked. Default 14 (days).
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

    # This is the main functionality of the check. Factored out for easier testing.
    res = lifecycle_utils.check_file_lifecycle_status(num_files_to_check, first_check_after, max_checking_frequency, my_auth)

    check.status = res["status"]
    check.summary = f'{len(res["files_to_update"])} files require patching.'

    check.full_output = {
        "files_to_update": res["files_to_update"],
        "files_without_update": res["files_without_update"],
        "files_with_issues": res["files_with_issues"],
        "files_not_being_checked": res["files_not_being_checked"],
        "logs": res["logs"]
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
                raise Exception(f'Could not determine S3 tag for file {uuid}')
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

