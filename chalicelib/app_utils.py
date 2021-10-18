from os.path import dirname
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from .vars import FOURSIGHT_PREFIX, HOST


class AppUtils(AppUtils_from_core):
    
    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = 'https://cgap.hms.harvard.edu/static/img/favicon-fs.ico'
    host = HOST
    package_name = 'chalicelib'
    check_setup_dir = dirname(__file__)
    html_main_title = 'Foursight-CGAP'
