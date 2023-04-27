import datetime

from dcicutils import ff_utils

## Schema constants ##

# lifecycle categories
SHORT_TERM_ACCESS_LONG_TERM_ARCHIVE = "short_term_access_long_term_archive"
LONG_TERM_ACCESS_LONG_TERM_ARCHIVE = "long_term_access_long_term_archive"
LONG_TERM_ACCESS = "long_term_access"
SHORT_TERM_ACCESS = "short_term_access"
LONG_TERM_ARCHIVE = "long_term_archive"
SHORT_TERM_ARCHIVE = "short_term_archive"
NO_STORAGE = "no_storage"
IGNORE = "ignore"


MOVE_TO_INFREQUENT_ACCESS_AFTER = "move_to_infrequent_access_after"
MOVE_TO_GLACIER_AFTER = "move_to_glacier_after"
MOVE_TO_DEEP_ARCHIVE_AFTER = "move_to_deep_archive_after"
EXPIRE_AFTER = "expire_after"
STANDARD = "standard"
INFREQUENT_ACCESS = "infrequent access"
GLACIER = "glacier"
DEEP_ARCHIVE = "deep archive"
DELETED = "deleted"
PENDING = "pending"
COMPLETE = "complete"
UPLOADED = "uploaded"
ARCHIVED = "archived"


DEFAULT_LIFECYCLE_POLICY = {
    SHORT_TERM_ACCESS_LONG_TERM_ARCHIVE: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,  # units in months
        MOVE_TO_DEEP_ARCHIVE_AFTER: 3,
    },
    LONG_TERM_ACCESS_LONG_TERM_ARCHIVE: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        MOVE_TO_DEEP_ARCHIVE_AFTER: 12,
    },
    SHORT_TERM_ACCESS: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        EXPIRE_AFTER: 12,
    },
    LONG_TERM_ACCESS: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
    },
    SHORT_TERM_ARCHIVE: {
        MOVE_TO_DEEP_ARCHIVE_AFTER: 0,
        EXPIRE_AFTER: 12,
    },
    LONG_TERM_ARCHIVE: {
        MOVE_TO_DEEP_ARCHIVE_AFTER: 0,
    },
    NO_STORAGE: {
        EXPIRE_AFTER: 0,
    },
}


def check_file_lifecycle_status(
    num_files_to_check, first_check_after, max_checking_frequency, my_auth
):
    """
    This main lifecycle check function. Factored out for easier testing
    """

    check_result = {"status": "PASS", "warning": ""}

    # We only want to get files from the portal that have a lifecycle category set and have either never been checked
    # or previously checked sufficiently long ago - as far as I know this can't be combined into one query. Furthermore,
    # they should be at least {first_check_after} days old
    threshold_date_fca = datetime.date.today() - datetime.timedelta(first_check_after)
    threshold_date_fca = threshold_date_fca.strftime("%Y-%m-%d")
    threshold_date_mcf = datetime.date.today() - datetime.timedelta(
        max_checking_frequency
    )
    threshold_date_mcf = threshold_date_mcf.strftime("%Y-%m-%d")

    search_query_base = (
        "/search/?type=File"
        "&project.lifecycle_management_active=true"
        "&status%21=deleted"
        "&status%21=archived"
        "&status%21=uploading"
        "&status%21=to+be+uploaded+by+workflow"
        "&s3_lifecycle_category%21=No+value"
        f"&s3_lifecycle_category%21={IGNORE}"
        f"&date_created.to={threshold_date_fca}"
        f"&limit={num_files_to_check // 2}"
    )
    search_query_1 = (
        f"{search_query_base}&s3_lifecycle_last_checked.to={threshold_date_mcf}"
    )
    search_query_2 = f"{search_query_base}&s3_lifecycle_last_checked=No+value"

    all_files = ff_utils.search_metadata(search_query_1, key=my_auth)
    all_files += ff_utils.search_metadata(search_query_2, key=my_auth)

    files_to_update = []  # This will contain the files that require lifecycle updates
    files_without_update = []
    files_with_issues = []
    logs = []

    # This dict will contain all the lifecycle policies per project that are relevant
    # for the current set of files. "project.lifecycle_policy" is not embedded in the File
    # item and we want to retrieve the project metadata only once for each project.
    lifecycle_policies_by_project = {}

    for file in all_files:
        file_uuid = file["uuid"]

        # Get the correct lifecycle policy - load it from the metadata only once
        project_uuid = file["project"]["uuid"]
        if project_uuid not in lifecycle_policies_by_project:
            project = ff_utils.get_metadata(project_uuid, key=my_auth)
            if "lifecycle_policy" in project:
                lifecycle_policies_by_project[project_uuid] = project[
                    "lifecycle_policy"
                ]
            else:
                lifecycle_policies_by_project[project_uuid] = DEFAULT_LIFECYCLE_POLICY

        lifecycle_policy = lifecycle_policies_by_project[project_uuid]

        file_lifecycle_category = file[
            "s3_lifecycle_category"
        ]  # e.g. "long_term_archive"
        if file_lifecycle_category not in lifecycle_policy:
            check_result["status"] = "WARN"
            check_result[
                "warning"
            ] = "Some files have unknown lifecycle categories. Check logs."
            logs.append(
                f"File {file_uuid} has an unknown lifecycle category {file_lifecycle_category}"
            )
            files_with_issues.append(file_uuid)
            continue

        # This contains the applicable rules for the current file, e.g., {MOVE_TO_DEEP_ARCHIVE_AFTER: 0, EXPIRE_AFTER: 12}
        file_lifecycle_policy = lifecycle_policy[file_lifecycle_category]

        file_old_lifecycle_status = file.get("s3_lifecycle_status", STANDARD)
        file_new_lifecycle_status = get_file_lifecycle_status(
            file, file_lifecycle_policy
        )

        # Check that the new storage class is indeed "deeper" than the old one. We can't transfer files to more accessible storage classes
        file_old_lifecycle_status_int = lifecycle_status_to_int(
            file_old_lifecycle_status
        )
        file_new_lifecycle_status_int = lifecycle_status_to_int(
            file_new_lifecycle_status
        )
        if file_old_lifecycle_status_int > file_new_lifecycle_status_int:
            check_result["status"] = "WARN"
            check_result[
                "warning"
            ] = "Unsupported storage class transition for some files. Check logs"
            logs.append(
                f"File {file_uuid} wants to transition from {file_old_lifecycle_status} to {file_new_lifecycle_status}"
            )
            files_with_issues.append(file_uuid)
            continue

        if file_old_lifecycle_status != file_new_lifecycle_status:
            update_dicts = get_update_dicts(file, file_new_lifecycle_status)
            files_to_update += update_dicts
        else:
            files_without_update.append(file_uuid)

    check_result["files_to_update"] = files_to_update
    check_result["files_without_update"] = files_without_update
    check_result["files_with_issues"] = files_with_issues
    check_result["logs"] = logs
    return check_result


def check_deleted_files_lifecycle_status(num_files_to_check, check_after, my_auth):
    """
    This is the lifecycle check function for deleted files.
    """

    check_result = {"status": "PASS", "warning": ""}

    # We only want to get deleted files from the portal that don't have a lifecycle category and have not been
    # modified for at least {first_check_after} days.
    threshold_date = datetime.date.today() - datetime.timedelta(check_after)
    threshold_date = threshold_date.strftime("%Y-%m-%d")

    search_query = (
        "/search/?type=File"
        "&project.lifecycle_management_active=true"
        f"&s3_lifecycle_status%21={DELETED}"
        f"&last_modified.date_modified.to={threshold_date}"
        f"&status={DELETED}"
        f"&limit={num_files_to_check}"
    )

    all_files = ff_utils.search_metadata(search_query, key=my_auth)

    files_to_update = []  # This will contain the files that require lifecycle updates
    file_new_lifecycle_status = DELETED

    for file in all_files:
        update_dicts = get_update_dicts(file, file_new_lifecycle_status)
        files_to_update += update_dicts

    check_result["files_to_update"] = files_to_update
    return check_result


def get_update_dicts(file, new_lifecycle_status):
    update_dicts = []
    update_dict = {
        "uuid": file["uuid"],
        "upload_key": file["upload_key"],
        "old_lifecycle_status": file.get("s3_lifecycle_status", STANDARD),
        "new_lifecycle_status": new_lifecycle_status,
        "is_extra_file": False,
    }
    update_dicts.append(update_dict)

    # Get extra files and update those as well. They will be treated like the original file
    extra_files = file.get("extra_files", [])
    for ef in extra_files:
        ef_update_dict = update_dict.copy()
        ef_update_dict["upload_key"] = ef["upload_key"]
        ef_update_dict["is_extra_file"] = True
        update_dicts.append(ef_update_dict)

    return update_dicts


# Factored out, so that it can be mocked in tests. Not pretty, but seemed to be the easiest solution
def get_datetime_utcnow():
    return datetime.datetime.utcnow()


def get_file_lifecycle_status(file, file_lifecycle_policy):
    """This function returns the correct lifecycle status for a given file, i.e.
       which S3 storage class it should currently be in.

    Args:
        file(dict) : file meta data from portal
        file_lifecycle_policy (dict) : Policy for that file, e.g. {MOVE_TO_DEEP_ARCHIVE_AFTER: 0, EXPIRE_AFTER: 12}

    Returns:
        string : correct lifecycle status of file given its lifecycle policy and age
    """

    date_created = convert_es_timestamp_to_datetime(file.get("date_created"))
    now = get_datetime_utcnow()
    # We are using the file creation for simplicity, we should use the time from when the workflow run
    # completed successfully (at least for submitted files). We asssume that those dates are sufficiently close.
    file_age = (now - date_created).days / 30  # in months

    # Find the lifecycle policy category that is currently applicable
    active_categories = {
        k: v for (k, v) in file_lifecycle_policy.items() if v < file_age
    }

    # File is younger than anything in the policy
    if not active_categories:
        return STANDARD

    current_category = max(active_categories, key=active_categories.get)

    return lifecycle_policy_to_status(current_category)


def lifecycle_policy_to_status(policy_category):
    """Converts a lifecycle policy category (e.g. "move_to_deep_archive_after") to the corresponding status (e.g. "deep archive").

    Args:
        policy_category(string): lifecycle policy category (e.g. "move_to_deep_archive_after")

    Returns:
        A string : lifecylce status (defaults to "standard")
    """
    if policy_category == MOVE_TO_INFREQUENT_ACCESS_AFTER:
        return INFREQUENT_ACCESS
    elif policy_category == MOVE_TO_GLACIER_AFTER:
        return GLACIER
    elif policy_category == MOVE_TO_DEEP_ARCHIVE_AFTER:
        return DEEP_ARCHIVE
    elif policy_category == EXPIRE_AFTER:
        return DELETED
    else:
        return STANDARD


def lifecycle_status_to_s3_tag(lifecycle_status):
    """Converts a lifecycle status to the corresponding S3 tag.

    Args:
        lifecycle_status(string): lifecycle status from portal (e.g. "deep archive")

    Returns:
        A list of tags (dicts) : S3 tags (defaults to empty list)
    """
    if lifecycle_status == INFREQUENT_ACCESS:
        return [{"Key": "Lifecycle", "Value": "IA"}]
    elif lifecycle_status == GLACIER:
        return [{"Key": "Lifecycle", "Value": "Glacier"}]
    elif lifecycle_status == DEEP_ARCHIVE:
        return [{"Key": "Lifecycle", "Value": "GlacierDA"}]
    elif lifecycle_status == DELETED:
        return [{"Key": "Lifecycle", "Value": "expire"}]
    else:
        return []


def lifecycle_status_to_int(lifecycle_status):
    """Converts a lifecycle status to an integer which represents how accessible the storage class is.
       Smaller number means more accessible

    Args:
        lifecycle_status(string): lifecycle status from portal (e.g. "deep archive")

    Returns:
        an integer
    """

    if lifecycle_status == INFREQUENT_ACCESS:
        return 2
    elif lifecycle_status == GLACIER:
        return 3
    elif lifecycle_status == DEEP_ARCHIVE:
        return 4
    elif lifecycle_status == DELETED:
        return 5
    else:
        return 1


def lifecycle_status_to_file_status(lifecycle_status):
    """Converts a lifecycle status to a file status.

    Args:
        lifecycle_status(string): lifecycle status from portal (e.g. "deep archive")

    Returns:
        File status
    """

    if lifecycle_status == GLACIER:
        return ARCHIVED
    elif lifecycle_status == DEEP_ARCHIVE:
        return ARCHIVED
    elif lifecycle_status == DELETED:
        return DELETED
    else:
        return False


def convert_es_timestamp_to_datetime(raw):
    """Convert the ElasticSearch timestamp to a Python Datetime.

    Args:
        raw(string): The ElasticSearch timestamp, as a string.

    Returns:
        A datetime object (or None)
    """
    converted_date = None

    if not raw:
        return converted_date

    index = raw.rfind(".")
    if index != -1:
        formatted_date = raw[0:index]
    else:
        formatted_date = raw

    converted_date = datetime.datetime.strptime(formatted_date, "%Y-%m-%dT%H:%M:%S")
    return converted_date
