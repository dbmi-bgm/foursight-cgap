from .vars import (
    FOURSIGHT_PREFIX,
    FAVICON as CurrentFavicon,
    HOST as CurrentHost
)
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from .check_utils import CheckHandler as CurrentCheckHandlerClass


class AppUtils(AppUtils_from_core):
    
    # these must be overwritten in inherited classes
    prefix = FOURSIGHT_PREFIX
    FAVICON = CurrentFavicon
    CheckHandler = CurrentCheckHandlerClass
    host = CurrentHost
    html_main_title = 'Foursight-CGAP'
