from os.path import dirname
from foursight_core.check_utils import CheckHandler as CheckHandler_from_core
from .vars import FOURSIGHT_PREFIX


class CheckHandler(CheckHandler_from_core):

    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    setup_dir = dirname(__file__)
    check_package_name = 'chalicelib'

    @classmethod
    def get_module_names(cls):
        from .checks import __all__ as check_modules
        return check_modules
