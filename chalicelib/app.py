from chalice import Cron
import os
from chalicelib.app_utils import AppUtils
from foursight_core.app_utils import app
from chalicelib.deploy import Deploy
app.debug = True
STAGE = os.environ.get('chalice_stage', 'dev')
DEFAULT_ENV = os.environ.get("ENV_NAME", "cgap-unknown")
app_utils_obj = AppUtils.singleton(AppUtils)


######### SCHEDULED FUNCTIONS #########

def effectively_never():
    """Every February 31st, a.k.a. 'never'."""
    return Cron('0', '0', '31', '2', '?', '?')


def friday_at_8_pm_est():
    """ Creates a Cron schedule (in UTC) for Friday at 8pm EST """
    return Cron('0', '0', '?', '*', 'SAT', '*')  # 24 - 4 = 20 = 8PM


def monday_at_2_am_est():
    """ Creates a Cron schedule (in UTC) for every Monday at 2 AM EST """
    return Cron('0', '6', '?', '*', 'MON', '*')  # 6 - 4 = 2AM


def end_of_day_on_weekdays():
    """ Cron schedule that runs at 11pm EST (03:00 UTC) on weekdays. Used for deployments. """
    return Cron('0', '3', '?', '*', 'TUE-SAT', '*')


# this dictionary defines the CRON schedules for the dev and prod foursight
# stagger them to reduce the load on Fourfront. Times are UTC
# info: https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
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
        'friday_autoscaling_checks': friday_at_8_pm_est(),
        'monday_autoscaling_checks': monday_at_2_am_est(),
        'manual_checks': effectively_never(),
        'deployment_checks': end_of_day_on_weekdays()
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
        'friday_autoscaling_checks': friday_at_8_pm_est(),  # disabled, see schedule below
        'monday_autoscaling_checks': monday_at_2_am_est(),  # disabled, see schedule below
        'manual_checks': effectively_never(),
        'deployment_checks': end_of_day_on_weekdays()  # disabled, see schedule below
    }
}


@app.schedule(foursight_cron_by_schedule[STAGE]['ten_min_checks'])
def ten_min_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'ten_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['fifteen_min_checks'])
def fifteen_min_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'fifteen_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['fifteen_min_checks_2'])
def fifteen_min_checks_2(event):
    app_utils_obj.queue_scheduled_checks('all', 'fifteen_min_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['fifteen_min_checks_3'])
def fifteen_min_checks_3(event):
    app_utils_obj.queue_scheduled_checks('all', 'fifteen_min_checks_3')


@app.schedule(foursight_cron_by_schedule[STAGE]['thirty_min_checks'])
def thirty_min_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'thirty_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks'])
def hourly_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'hourly_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks_2'])
def hourly_checks_2(event):
    app_utils_obj.queue_scheduled_checks('all', 'hourly_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['early_morning_checks'])
def early_morning_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'early_morning_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks'])
def morning_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'morning_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks_2'])
def morning_checks_2(event):
    app_utils_obj.queue_scheduled_checks('all', 'morning_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['evening_checks'])
def evening_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'evening_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['monday_checks'])
def monday_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'monday_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['monthly_checks'])
def monthly_checks(event):
    app_utils_obj.queue_scheduled_checks('all', 'monthly_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['deployment_checks'])
def deployment_checks(event):
    if STAGE == 'dev':
        return  # do not schedule the deployment checks on dev
    app_utils_obj.queue_scheduled_checks('all', 'deployment_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['friday_autoscaling_checks'])
def friday_autoscaling_checks(event):
    if STAGE == 'dev':
        return  # do not schedule autoscaling checks on dev
    app_utils_obj.queue_scheduled_checks('all', 'friday_autoscaling_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['monday_autoscaling_checks'])
def monday_autoscaling_checks(event):
    if STAGE == 'dev':
        return  # do not schedule autoscaling checks on dev
    app_utils_obj.queue_scheduled_checks('all', 'monday_autoscaling_checks')


######### MISC UTILITY FUNCTIONS #########

def set_stage(stage):
    if stage != 'test' and stage not in Deploy.CONFIG_BASE['stages']:
        print('ERROR! Input stage is not valid. Must be one of: %s' % str(list(Deploy.CONFIG_BASE['stages'].keys()).extend('test')))
    os.environ['chalice_stage'] = stage


def set_timeout(timeout):
    app_utils_obj.set_timeout(timeout)
