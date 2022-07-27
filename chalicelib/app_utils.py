import os
from os.path import dirname
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from .vars import FOURSIGHT_PREFIX, HOST


class AppUtils(AppUtils_from_core):

    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = 'https://cgap-dbmi.hms.harvard.edu/favicon.ico'
    host = HOST
    package_name = 'chalicelib'
    check_setup_dir = dirname(__file__)
    html_main_title = 'Foursight-CGAP'

    # dmichaels/2022-07-20/C4-826:
    # Added this to get the ES_HOST value from via IDENTITY (from GAC).
    # which was applied globally (to os.environ) in foursight-core,
    # via AppUtils_from_core; to override the default self.host
    # value set above to the correct value.
    def __init__(self):
        super().__init__()
        es_host_from_foursight_core_apply_identity = os.environ.get("ES_HOST")
        if es_host_from_foursight_core_apply_identity:
            self.host = es_host_from_foursight_core_apply_identity
