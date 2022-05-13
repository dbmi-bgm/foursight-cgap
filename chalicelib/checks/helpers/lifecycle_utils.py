import json
from datetime import datetime
from dcicutils import ff_utils, s3Utils
import pprint

pp = pprint.PrettyPrinter(indent=2)

# Schema constants
RESULT = "result"
PERSISTENT_RESULT = "persistent_result"
READS = "reads"
INTERMEDIATE_OUTPUT = "intermediate_output"
INTERMEDIATE_RESULT = "intermediate_result"
MOVE_TO_INFREQUENT_ACCESS_AFTER = "move_to_infrequent_access_after"
MOVE_TO_DEEP_ARCHIVE_AFTER = "move_to_deep_archive_after"
EXPIRE_AFTER = "expire_after"
STANDARD = "standard"
INFREQUENT_ACCESS = "infrequent access"
DEEP_ARCHIVE = "deep archive"
DELETED = "deleted"
IGNORE = "ignore"
PENDING = "pending"
COMPLETE = "complete"

# lifecycle_status = {
#     "standard": "Standard",
#     "ia": "Infrequent Access",
#     "da": "Deep Archive",
#     "deleted": "Deleted",
# }

default_lifecycle_policy = {
    RESULT: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        MOVE_TO_DEEP_ARCHIVE_AFTER: 3,
        EXPIRE_AFTER: 36,
    },
    PERSISTENT_RESULT: {
        MOVE_TO_INFREQUENT_ACCESS_AFTER: 0,
        EXPIRE_AFTER: 36,
    },
    READS: {
        MOVE_TO_DEEP_ARCHIVE_AFTER: 0,
        EXPIRE_AFTER: 12,
    },
    INTERMEDIATE_OUTPUT: {
        EXPIRE_AFTER: 0,
    },
    INTERMEDIATE_RESULT: {
        MOVE_TO_DEEP_ARCHIVE_AFTER: 0,
        EXPIRE_AFTER: 36,
    },
}





def should_mwfr_be_checked(metawfr, max_checking_frequency):
    """This function determines if a MetaWorkflowRun should be checked for lifecycle updates

    Args:
        metawfr(dict): MetaWorkflowRun from portal
        max_checking_frequency (int): determines how often a metawfr is checked at most (in days). Default 7 (days).

    Returns:
        boolean
    """
    # TODO: Add stopped, inactive here as well?
    valid_final_status = ["completed"]  # Only these are checked in the following
    if metawfr["final_status"] not in valid_final_status:
        return False

    # Check when the MWFR has been created. If it is younger than 2 weeks, don't check it
    now = datetime.utcnow()
    date_created = convert_es_timestamp_to_datetime(metawfr["date_created"])
    metawfr_age = now - date_created
    if metawfr_age.total_seconds() < 14 * 24 * 60 * 60:
        return False

    # If lifecycle_status is present, the metawfr was been checked before.
    # Make sure enough time passed to check it again
    # TODO Check the following, once the metatdata is there
    if "lifecycle_status" in metawfr:
        metawfr_lifecycle_info = metawfr["lifecycle_status"]
        last_checked = convert_es_timestamp_to_datetime(metawfr_lifecycle_info["last_checked"])
        status = metawfr_lifecycle_info["status"]
        delta = now - last_checked
        # check this metawfr at most every {max_checking_frequency} days
        if delta.total_seconds() < max_checking_frequency * 24 * 60 * 60:
            return False

        if status == "pending":
            return True

    return False



def get_lifecycle_category(file_metadata):
    """This function assigns a lifecycle category to to a given file. 
    It is currently only working for certain types of files and return None for all other files

    Args:
        file_metadata(dict): file metadata from portal

    Returns:
        A string : lifecylce category (defaults to None)
    """
    file_format = file_metadata.get("file_format").get("file_format")
    file_type = file_metadata.get("file_type")
    if file_format in ["fastq", "cram"]:
        return READS
    elif file_format in ["bam"] and file_type == "alignments":
        return RESULT
    elif file_format in ["bam"] and file_type == "intermediate file":
        return INTERMEDIATE_OUTPUT
    else:
        return None


def get_workflow_lifecycle_category_map(meta_workflow):
    """This function extracts a mapping "workflow name -> lifecycle category" from a meta workflow
    as stored in the custom PF field of the workflow. This category represents the lifecycle category
    of the output files of the corresponding workflow

    Args:
        meta_workflow(dict): meta workflow from portal

    Returns:
        dict : mapping workflow name -> output files lifecycle category
    """
    workflows = meta_workflow.get("workflows")
    #pp.pprint(workflows)
    mapping = {}

    for workflow in workflows:
        if "custom_pf_fields" not in workflow:
            continue
        custom_pf_fields = workflow["custom_pf_fields"]
        if "output_files_lifecycle_catgory" not in custom_pf_fields:
            continue
        mapping[workflow["name"]] = custom_pf_fields["output_files_lifecycle_catgory"]

    return mapping


def get_file_lifecycle_status(file_metadata, file_lifecycle_policy):
    """This function returns the correct lifecycle status for a given file, i.e.
       which S3 storage class it should currently be in.

    Args:
        file_metadata(dict) : file meta data from portal
        file_lifecycle_policy (dict) : Policy for that file, e.g. {MOVE_TO_DEEP_ARCHIVE_AFTER: 0, EXPIRE_AFTER: 12}

    Returns:
        string : correct lifecycle status of file given its lifecycle policy and age
    """

    date_created = convert_es_timestamp_to_datetime(file_metadata.get("date_created"))
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
    elif lifecycle_status == DEEP_ARCHIVE:
        return [{'Key': 'Lifecycle','Value': 'GlacierDA'}]
    elif lifecycle_status == DELETED:
        return [{'Key': 'Lifecycle','Value': 'expire'}]
    else:
        return []


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
