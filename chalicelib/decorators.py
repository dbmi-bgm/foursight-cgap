from foursight_core.decorators import Decorators as Decorators_from_core
from .run_result import (
    CheckResult as CurrentCheckResultClass,
    ActionResult as CurrentActionResultClass,
)
from .sqs_utils import SQS as CurrentSQSClass


class Decorators(Decorators_from_core):

    CheckResult = CurrentCheckResultClass
    ActionResult = CurrentActionResultClass
    SQS = CurrentSQSClass
