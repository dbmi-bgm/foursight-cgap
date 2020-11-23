from os.path import dirname
from foursight_core.chalicelib.check_utils import CheckHandler as CheckHandler_from_core
from .environment import Environment as CurrentEnvironmentClass
from .run_result import (
    CheckResult as CurrentCheckResultClass,
    ActionResult as CurrentActionResultClass
)


class CheckHandler(CheckHandler_from_core):

    # these must be overwritten for inherited classes
    setup_dir = dirname(__file__)
    CheckResult = CurrentCheckResultClass
    ActionResult = CurrentActionResultClass
    Environment = CurrentEnvironmentClass
    check_package_name = 'chalicelib'

    @classmethod
    def get_module_names(cls):
        from .checks import __all__ as check_modules
        return check_modules
