import json
from datetime import datetime
from dcicutils import ff_utils, s3Utils
import pprint

pp = pprint.PrettyPrinter(indent=2)

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


default_lifecycle_policy = {
    SHORT_TERM_ACCESS_LONG_TERM_ARCHIVE: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        MOVE_TO_DEEP_ARCHIVE_AFTER: 3,
        EXPIRE_AFTER: 36,
    },
    LONG_TERM_ACCESS_LONG_TERM_ARCHIVE: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        MOVE_TO_DEEP_ARCHIVE_AFTER: 12,
        EXPIRE_AFTER: 36,
    },
    SHORT_TERM_ACCESS: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        EXPIRE_AFTER: 12,
    },
    LONG_TERM_ACCESS: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        EXPIRE_AFTER: 36,
    },
    SHORT_TERM_ARCHIVE: {
        MOVE_TO_DEEP_ARCHIVE_AFTER: 0,
        EXPIRE_AFTER: 12,
    },
    LONG_TERM_ARCHIVE: {
        MOVE_TO_DEEP_ARCHIVE_AFTER: 0,
        EXPIRE_AFTER: 36,
    },
    NO_STORAGE: {
        EXPIRE_AFTER: 0,
    },
}

lifecycle_status_to_file_status = {
    STANDARD: UPLOADED,
    INFREQUENT_ACCESS: UPLOADED,
    GLACIER: ARCHIVED,
    DEEP_ARCHIVE: ARCHIVED,
    DELETED: DELETED
}


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
    now = datetime.utcnow()
    # We are using the file creation for simplicity, we should use the time from when the workflow run
    # completed successfully. We asssume that those dates are sufficiently close.
    file_age = (now - date_created).days / 30  # in months

    # Find the lifecycle policy category that is currently applicable
    active_categories = {k: v for (k, v) in file_lifecycle_policy.items() if v < file_age}
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
        return [{'Key': 'Lifecycle','Value': 'IA'}]
    elif lifecycle_status == GLACIER:
        return [{'Key': 'Lifecycle','Value': 'Glacier'}]
    elif lifecycle_status == DEEP_ARCHIVE:
        return [{'Key': 'Lifecycle','Value': 'GlacierDA'}]
    elif lifecycle_status == DELETED:
        return [{'Key': 'Lifecycle','Value': 'expire'}]
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
    mapping = {
        STANDARD: 1,
        INFREQUENT_ACCESS: 2,
        GLACIER: 3,
        DEEP_ARCHIVE: 4,
        DELETED: 5
    }
    
    return mapping[lifecycle_status]


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

    converted_date = datetime.strptime(formatted_date, "%Y-%m-%dT%H:%M:%S")
    return converted_date
