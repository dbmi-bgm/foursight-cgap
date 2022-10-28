from chalice import Cron
import os
from os.path import dirname
from dcicutils.exceptions import InvalidParameterError
from dcicutils.misc_utils import ignored
from foursight_core.app_utils import app # Chalice object
from foursight_core.app_utils import AppUtilsCore as AppUtils_from_core
from foursight_core.deploy import Deploy
from foursight_core.identity import apply_identity_globally
from .vars import FOURSIGHT_PREFIX, HOST


class AppUtils(AppUtils_from_core):

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
    check_setup_dir = dirname(__file__)
    check_setup_dir_fallback = dirname(__file__)
    DEFAULT_ENV = os.environ.get("ENV_NAME", "foursight-cgap-env-uninitialized")
    #html_main_title = f'Foursight-{DEFAULT_ENV}'.title().replace("Cgap", "CGAP")
    html_main_title = "Foursight" # Foursight CGAP vs Fourfront difference now conveyed in the upper left icon.

# --------------------------------------------------------------------------------------------------
# Previously in: 4dn-cloud-infra
# But note we do not access the main AppUtils object via AppUtils.singleton(AppUtils) but rather
# via app.core (where app is the Chalice object from foursight_core), which is set in foursight_core
# constructor; the AppUtils.singleton is invoked from 4dn-cloud-infra/app.py to make sure it gets
# the AppUtils derivation there (yes that singleton is odd in taking a class argument). We could
# actually reference AppUtils.singleton here, but not at the file level, only within functions,
# below, but best not to use it at all here to reduce confusion.
# --------------------------------------------------------------------------------------------------

# TODO: This is how it was gotten in 4dn-cloud-infra ... Is this okay here too?
STAGE = os.environ.get('chalice_stage', 'dev')

######### SCHEDULED FUNCTIONS #########

def effectively_never():
    """Every February 31st, a.k.a. 'never'."""
    return Cron('0', '0', '31', '2', '?', '*')


def morning_10am_utc():
    """ Schedule for every morning at 10AM UTC (6AM EST) """
    return Cron('0', '10', '*', '*', '?', '*')


foursight_cron_by_schedule = {
    'prod': {
        'ten_min_checks': Cron('0/10', '*', '*', '*', '?', '*'),
        'fifteen_min_checks': Cron('0/15', '*', '*', '*', '?', '*'),
        'fifteen_min_checks_2': Cron('5/15', '*', '*', '*', '?', '*'),
        'fifteen_min_checks_3': Cron('10/15', '*', '*', '*', '?', '*'),
        'thirty_min_checks': Cron('0/30', '*', '*', '*', '?', '*'),
        'hourly_checks': Cron('0', '0/1', '*', '*', '?', '*'),
        'hourly_checks_2': Cron('15', '0/1', '*', '*', '?', '*'),
        'early_morning_checks': Cron('0', '8', '*', '*', '?', '*'),
        'morning_checks': Cron('0', '10', '*', '*', '?', '*'),
        'morning_checks_2': Cron('15', '10', '*', '*', '?', '*'),
        'evening_checks': Cron('0', '22', '*', '*', '?', '*'),
        'monday_checks': Cron('0', '9', '?', '*', '2', '*'),
        'monthly_checks': Cron('0', '9', '1', '*', '?', '*'),
        'manual_checks': effectively_never(),
    },
    'dev': {
        'ten_min_checks': Cron('5/10', '*', '*', '*', '?', '*'),
        'fifteen_min_checks': Cron('0/15', '*', '*', '*', '?', '*'),
        'fifteen_min_checks_2': Cron('5/15', '*', '*', '*', '?', '*'),
        'fifteen_min_checks_3': Cron('10/15', '*', '*', '*', '?', '*'),
        'thirty_min_checks': Cron('15/30', '*', '*', '*', '?', '*'),
        'hourly_checks': Cron('30', '0/1', '*', '*', '?', '*'),
        'hourly_checks_2': Cron('45', '0/1', '*', '*', '?', '*'),
        'early_morning_checks': Cron('0', '8', '*', '*', '?', '*'),
        'morning_checks': Cron('30', '10', '*', '*', '?', '*'),
        'morning_checks_2': Cron('45', '10', '*', '*', '?', '*'),
        'evening_checks': Cron('0', '22', '*', '*', '?', '*'),
        'monday_checks': Cron('30', '9', '?', '*', '2', '*'),
        'monthly_checks': Cron('30', '9', '1', '*', '?', '*'),
        'manual_checks': effectively_never(),
    }
}

#@app.lambda_function()
#def check_runner(event, context):
#    """
#    Pure lambda function to pull run and check information from SQS and run
#    the checks. Self propogates. event is a dict of information passed into
#    the lambda at invocation time.
#    """
#    if not event:
#        return
#    app.core.run_check_runner(event)


@app.schedule(foursight_cron_by_schedule[STAGE]['manual_checks'])
def manual_checks():
    app.core.queue_scheduled_checks('all', 'manual_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks'])
def morning_checks(event):
    ignored(event)
    app.core.queue_scheduled_checks('all', 'morning_checks')


#@app.schedule(foursight_cron_by_schedule[STAGE]['fifteen_min_checks'])
#def fifteen_min_checks(event):
#    ignored(event)
#    app.core.queue_scheduled_checks('all', 'fifteen_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['fifteen_min_checks_2'])
def fifteen_min_checks_2(event):
    ignored(event)
    app.core.queue_scheduled_checks('all', 'fifteen_min_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['fifteen_min_checks_3'])
def fifteen_min_checks_3(event):
    ignored(event)
    app.core.queue_scheduled_checks('all', 'fifteen_min_checks_3')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks'])
def hourly_checks(event):
    ignored(event)
    app.core.queue_scheduled_checks('all', 'hourly_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks_2'])
def hourly_checks_2(event):
    ignored(event)
    app.core.queue_scheduled_checks('all', 'hourly_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['monthly_checks'])
def monthly_checks(event):
    ignored(event)
    app.core.queue_scheduled_checks('all', 'monthly_checks')


######### MISC UTILITY FUNCTIONS #########


def compute_valid_deploy_stages():
    # TODO: Will wants to know why "test" is here. -kmp 17-Aug-2021
    return list(Deploy.CONFIG_BASE['stages'].keys()) + ['test']


class InvalidDeployStage(InvalidParameterError):

    @classmethod
    def compute_valid_options(cls):
        return compute_valid_deploy_stages()


def set_stage(stage):
    if stage not in compute_valid_deploy_stages():
        raise InvalidDeployStage(parameter='stage', value=stage)
    os.environ['chalice_stage'] = stage


def set_timeout(timeout):
    app.core.set_timeout(timeout)
