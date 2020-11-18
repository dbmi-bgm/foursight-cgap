from __future__ import unicode_literals
from foursight_core.chalicelib.decorators import Decorators as _Decorators
from .run_result import CheckResult, ActionResult


class Decorators(_Decorators):

    CheckResult = CheckResult
    ActionResult = ActionResult
