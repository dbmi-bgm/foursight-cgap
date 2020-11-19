from __future__ import unicode_literals
from os.path import dirname
from foursight_core.chalicelib.check_utils import CheckHandler as _CheckHandler
from .config import Config
from .run_result import CheckResult, ActionResult


class CheckHandler(_CheckHandler):

    # these must be overwritten for inherited classes
    setup_dir = dirname(__file__)
    CheckResult = CheckResult
    ActionResult = ActionResult
    check_package_name = 'chalicelib'

    @classmethod
    def get_module_names(cls):
        from .checks import __all__ as check_modules
        return check_modules
