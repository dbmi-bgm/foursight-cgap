from os.path import dirname
from .vars import (
    FOURSIGHT_PREFIX,
    FAVICON as CurrentFavicon,
    HOST as CurrentHost
)
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from .run_result import (
    CheckResult as CurrentCheckResultClass,
    ActionResult as CurrentActionResultClass
)
from .check_utils import CheckHandler as CurrentCheckHandlerClass
from .sqs_utils import SQS as CurrentSQSClass
from .stage import Stage as CurrentStageClass
from .environment import Environment as CurrentEnvironmentClass


class AppUtils(AppUtils_from_core):
    
    # these must be overwritten in inherited classes
    prefix = FOURSIGHT_PREFIX
    FAVICON = CurrentFavicon
    Stage = CurrentStageClass
    Environment = CurrentEnvironmentClass
    CheckHandler = CurrentCheckHandlerClass
    CheckResult = CurrentCheckResultClass
    ActionResult = CurrentActionResultClass
    SQS = CurrentSQSClass
    host = CurrentHost

    template_dir = dirname(__file__)
