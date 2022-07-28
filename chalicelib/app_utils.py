import os
from os.path import dirname
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from .vars import FOURSIGHT_PREFIX, HOST


class AppUtils(AppUtils_from_core):

    es_host_from_foursight_core_apply_identity = os.environ.get("ES_HOST")
    if es_host_from_foursight_core_apply_identity:
        HOST = es_host_from_foursight_core_apply_identity

    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = 'https://cgap-dbmi.hms.harvard.edu/favicon.ico'
    host = HOST
    package_name = 'chalicelib'
    check_setup_dir = dirname(__file__)
    html_main_title = 'Foursight-CGAP'
