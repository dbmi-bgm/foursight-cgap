from foursight_core.run_result import (
    CheckResult as CheckResult_from_core,
    ActionResult as ActionResult_from_core,
    BadCheckOrAction
)
from .vars import FOURSIGHT_PREFIX


class CheckResult(CheckResult_from_core):
    """
    Inherits from CheckResult from core and is meant to be used with checks.

    Usage:
    check = CheckResult(connection, <name>)
    check.status = ...
    check.descritpion = ...
    check.store_result()
    """
    prefix = FOURSIGHT_PREFIX


class ActionResult(ActionResult_from_core):
    """
    Inherits from ActionResult from foursight_core and is meant to be used with actions
    """
    prefix = FOURSIGHT_PREFIX
