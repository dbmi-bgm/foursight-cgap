import os
from os.path import dirname
from foursight_core.app_utils import app  # Chalice object
from foursight_core.app_utils import AppUtilsCore as AppUtils_from_core
from foursight_core.identity import apply_identity_globally
from .vars import FOURSIGHT_PREFIX, HOST

check_setup_dir = os.environ.get("FOURSIGHT_CHECK_SETUP_DIR")

print('xyzzy;chalicelib_cgap..........................2')
print(check_setup_dir)

#TODO: Rename back to AppUtils with merged with 4dn-cloud-infra version below after verified working ...
class AppUtils_from_cgap_or_fourfront(AppUtils_from_core):

    # dmichaels/C4-826: Apply identity globally.
    apply_identity_globally()

    # Overridden from subclass.
    APP_PACKAGE_NAME = "foursight-cgap"

    # Note that this is set in the new (as of August 2022) apply_identity code;
    # see foursight-core/foursight_core/{app_utils.py,identity.py}.
    es_host = os.environ.get("ES_HOST")
    if not es_host:
        raise Exception("Foursight ES_HOST environment variable not set!")
    HOST = es_host

    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = 'https://cgap-dbmi.hms.harvard.edu/favicon.ico'
    host = HOST
    package_name = 'chalicelib_cgap'
    print('xyzzy;chalicelib_cgap..........................3')
    print(check_setup_dir)
    check_setup_dir = dirname(__file__)
    check_setup_dir_fallback = dirname(__file__)
    DEFAULT_ENV = os.environ.get("ENV_NAME", "foursight-cgap-env-uninitialized")
    html_main_title = "Foursight" # Foursight CGAP vs Fourfront difference now conveyed in the upper left icon.


# From 4dn-cloud-infra - TODO: merge with above after verify working ...

from dcicutils.misc_utils import remove_suffix

STAGE = os.environ.get('chalice_stage', 'dev')
HOST = os.environ.get('ES_HOST', None)

FOURSIGHT_PREFIX = os.environ.get('FOURSIGHT_PREFIX')
if not FOURSIGHT_PREFIX:
    _GLOBAL_ENV_BUCKET = os.environ.get('GLOBAL_ENV_BUCKET') or os.environ.get('GLOBAL_BUCKET_ENV')
    if _GLOBAL_ENV_BUCKET is not None:
        print('_GLOBAL_ENV_BUCKET=', _GLOBAL_ENV_BUCKET)  # TODO: Temporary print statement, for debugging
        FOURSIGHT_PREFIX = remove_suffix('-envs', _GLOBAL_ENV_BUCKET, required=True)
        print(f'Inferred FOURSIGHT_PREFIX={FOURSIGHT_PREFIX}')
    else:
        raise RuntimeError('The FOURSIGHT_PREFIX environment variable is not set. Heuristics failed.')


# This object usually in chalicelib_fourfront/app_utils.py
class AppUtils(AppUtils_from_cgap_or_fourfront):
    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = 'https://cgap-dbmi.hms.harvard.edu/static/img/favicon-fs.ico'
    host = HOST
    package_name = 'chalicelib_cgap'
    # check_setup is moved to vendor/ where it will be automatically placed at top level
    print('xyzzy;chalicelib_cgap..........................4')
    print(check_setup_dir)
    # check_setup_dir = os.path.dirname(__file__)
    check_setup_dir = os.environ.get("FOURSIGHT_CHECK_SETUP_DIR") or os.path.dirname(__file__)
    # html_main_title = f'Foursight-{DEFAULT_ENV}-{STAGE}'.title()
    # html_main_title = 'Foursight-Fourfront';
    html_main_title = "Foursight" # Foursight CGAP vs Fourfront difference now conveyed in the upper left icon.
    DEFAULT_ENV = os.environ.get('ENV_NAME', 'foursight-fourfront-env-uninitialized')


app_utils_obj = AppUtils.singleton(AppUtils)