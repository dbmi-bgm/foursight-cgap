from foursight_core.chalicelib.run_result import (
    RunResult as _RunResult,
    CheckResult as _CheckResult,
    ActionResult as _ActionResult,
    BadCheckOrAction
)
from .vars import FOURSIGHT_PREFIX


class RunResult(_RunResult):
    """
    Generic class for CheckResult and ActionResult. Contains methods common
    to both.
    """
    prefix = FOURSIGHT_PREFIX


class CheckResult(_CheckResult):
    """
    Inherits from RunResult and is meant to be used with checks.

    Usage:
    check = CheckResult(connection, <name>)
    check.status = ...
    check.descritpion = ...
    check.store_result()
    """
    prefix = FOURSIGHT_PREFIX


class ActionResult(_ActionResult):
    """
    Inherits from RunResult and is meant to be used with actions
    """
    prefix = FOURSIGHT_PREFIX
