from chalice import Cron
import os
from dcicutils.exceptions import InvalidParameterError
from foursight_core.app_utils import app  # Chalice object
from foursight_core.deploy import Deploy
from foursight_core.schedule_decorator import schedule, SCHEDULE_FOR_NEVER

# --------------------------------------------------------------------------------------------------
# Previously in: 4dn-cloud-infra
# But note we do not access the main AppUtils object via AppUtils.singleton(AppUtils) but
# rather via app.core (where app is the Chalice object from foursight_core), which is set
# in foursight_core constructor; the AppUtils.singleton is invoked from 4dn-cloud-infra/app.py
# to make sure it gets the AppUtils derivation there (yes that singleton is odd in taking a
# class argument). We could actually reference AppUtils.singleton here, but not at the file
# level, only within functions, below, but best not to use it at all here to reduce confusion.
# --------------------------------------------------------------------------------------------------

STAGE = os.environ.get("chalice_stage", "dev")
DISABLED_STAGES = []

SCHEDULES = {
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
        'manual_checks': SCHEDULE_FOR_NEVER
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
        'manual_checks': SCHEDULE_FOR_NEVER
    }
}

# New schedule decorator does not work yet ...

@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def manual_checks():
    app.core.queue_scheduled_checks('all', 'manual_checks')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def morning_checks(event):
    app.core.queue_scheduled_checks('all', 'morning_checks')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def fifteen_min_checks(event):
    app.core.queue_scheduled_checks('all', 'fifteen_min_checks')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def fifteen_min_checks_2(event):
    app.core.queue_scheduled_checks('all', 'fifteen_min_checks_2')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def fifteen_min_checks_3(event):
    app.core.queue_scheduled_checks('all', 'fifteen_min_checks_3')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def hourly_checks(event):
    app.core.queue_scheduled_checks('all', 'hourly_checks')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def hourly_checks_2(event):
    app.core.queue_scheduled_checks('all', 'hourly_checks_2')


@schedule(SCHEDULES, stage=STAGE, disabled_stages=DISABLED_STAGES)
def monthly_checks(event):
    app.core.queue_scheduled_checks('all', 'monthly_checks')


@app.lambda_function()
def check_runner(event, context):
    """
    Pure lambda function to pull run and check information from SQS and run
    the checks. Self propogates. event is a dict of information passed into
    the lambda at invocation time.
    """
    if not event:
        return
    app.core.run_check_runner(event)
