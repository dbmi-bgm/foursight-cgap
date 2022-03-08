import json
from datetime import datetime
from dcicutils import ff_utils, s3Utils
import pprint

pp = pprint.PrettyPrinter(indent=2)

lifecycle_status = {
    "standard": "Standard",
    "ia": "Infrequent Access",
    "da": "Deep Archive",
    "deleted": "Deleted",
}

default_lifecycle_policy = {
    "final_bam": {
        "move_to_ia_after": 0,
        "move_to_da_after": 3,
        "expire_after": 36,
    },
    "reads": {
        "move_to_da_after": 0,
        "expire_after": 12,
    },
    "intermediate_output": {
        "expire_after": 0,
    },
    "intermediate_result": {
        "move_to_da_after": 0,
        "expire_after": 36,
    },
}


# TODO To implement
def has_bam_qc_passed():
    return True


# TODO To implement
def are_variants_ingested():
    return True


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
        return "reads"
    elif file_format in ["bam"] and file_type == "alignments":
        return "final_bam"
    elif file_format in ["bam"] and file_type == "intermediate file":
        return "intermediate_output"
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

        



# get the correct lifecycle status for a file
def get_lifecycle_status(file_metadata, file_lifecycle_policy):

    date_created = convert_es_timestamp_to_datetime(file_metadata.get("date_created"))
    date_new = datetime.utcnow()
    # We are using the file creation for simplicity, we should use the time from when the workflow run
    # completed successfully. We asssume that those dates are sufficiently close.
    file_age = (date_new - date_created).days / 30  # in months
    #print(file_age, "months", date_created, date_new)
    #pp.pprint(file_lifecycle_policy)

    # Find the lifecycle policy category that is currently applicable
    active_categories = {k: v for (k, v) in file_lifecycle_policy.items() if v < file_age}
    current_category = max(active_categories, key=active_categories.get)

    # print(active_categories, current_category)
    # print(map_lifecycle_policy_to_status(current_category))
    # pp.pprint(file_metadata)
    return map_lifecycle_policy_to_status(current_category)


def map_lifecycle_policy_to_status(policy_category):
    """Converts a lifecycle policy category (e.g. "move_to_da_after") to the corresponding status (e.g. "da").

    Args:
        policy_category(string): lifecycle policy category (e.g. "move_to_da_after")

    Returns:
        A string : lifecylce status (defaults to "standard")
    """
    if policy_category == "move_to_ia_after":
        return "ia"
    elif policy_category == "move_to_da_after":
        return "da"
    elif policy_category == "expire_after":
        return "deleted"
    else:
        return "standard"


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
