import json
from datetime import datetime

from dcicutils import ff_utils

from . import constants
from .confchecks import CheckResult, ActionResult


def initialize_check(check_name, connection):
    """Create a CheckResult with default attributes.

    Set default status to error to be updated within check
    appropriately if all goes well.
    """
    check = CheckResult(connection, check_name)
    check.brief_output = []
    check.full_output = {}
    check.status = constants.CHECK_ERROR
    check.allow_action = True
    return check


def initialize_action(action_name, connection, kwargs):
    """Create an ActionResult with default attributes.

    Set default status to failure to be updated within action
    appropriately if all goes well.
    """
    action = ActionResult(connection, action_name)
    action.status = constants.ACTION_FAIL
    action.output = {}
    check_result = action.get_associated_check_result(kwargs).get("full_output", {})
    return action, check_result


def format_kwarg_list(kwarg_input):
    """Ensure kwarg is a set of unique strings."""
    if isinstance(kwarg_input, str):
        result = set()
        no_space_input = kwarg_input.replace(" ", ",")
        split_input = no_space_input.split(",")
        for input_item in split_input:
            stripped_item = input_item.strip()
            if stripped_item:
                result.add(stripped_item)
    elif isinstance(kwarg_input, list):
        result = set(kwarg_input)
    elif kwarg_input is None:
        result = set()
    else:
        raise Exception("Couldn't format kwarg input: %s" % kwarg_input)
    return result


def validate_items_existence(item_identifiers, connection):
    """Get raw view of items from database and keep track of which
    identifiers could not be retrieved.
    """
    found = []
    not_found = []
    if isinstance(item_identifiers, str):
        item_identifiers = [item_identifiers]
    for item_identifier in item_identifiers:
        try:
            item = ff_utils.get_metadata(
                item_identifier,
                key=connection.ff_keys,
                add_on="frame=raw",
            )
            found.append(item)
        except Exception:
            not_found.append(item_identifier)
    return found, not_found


def add_to_dict_as_list(dictionary, key, value):
    """Add key, value pair to dictionary, with values for key stored in
    list.
    """
    existing_item_value = dictionary.get(key)
    if existing_item_value:
        existing_item_value.append(value)
    else:
        dictionary[key] = [value]


def make_embed_request(ids, fields, connection):
    """POST to /embed API to get desired fields for all given
    identifiers.
    """
    result = []
    if isinstance(ids, str):
        ids = [ids]
    if isinstance(fields, str):
        fields = [fields]
    id_chunks = chunk_ids(ids, chunk_size=5)  # Max 5 IDs to /embed as of 20220601 -drr
    for id_chunk in id_chunks:
        post_body = {"ids": ids, "fields": fields}
        endpoint = connection.ff_server + "/embed"
        embed_response = ff_utils.authorized_request(
            endpoint, verb="POST", auth=connection.ff_keys, data=json.dumps(post_body)
        ).json()
        result += embed_response
    if len(result) == 1:
        result = result[0]
    return result


def chunk_ids(ids, chunk_size=5):
    """Split list into list of lists of maximum chunk size length."""
    result = []
    for idx in range(0, len(ids), chunk_size):
        result.append(ids[idx: idx + chunk_size])
    return result


def get_step_function_name(connection):
    """Create step function environment from given connection"""
    # XXX Acquire from health page in future?
    return "tibanna_zebra_" + connection.ff_env.replace("fourfront-", "")


def is_past_time_limit(start, limit):
    """Determine if time interval exceeds limit."""
    result = False
    now = datetime.utcnow()
    if (now - start).seconds > limit:
        result = True
    return result
