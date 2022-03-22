#import os
#FOURSIGHT_PREFIX = os.environ.get('GLOBAL_ENV_BUCKET') or os.environ.get('GLOBAL_BUCKET_ENV')
import app
# CHECK PARAMETERS
# SET CHECK
#check = 'system_checks/secondary_queue_deduplication'
#check = 'wfr_checks/md5run_status'
check = 'wfr_checks/md5run_status'
action = ''

# WHICH ENV YOU WANT TO WORK ON (data, staging, cgapwolf, ...)
env = 'cgap-wolf'

# DO YOU WANT FOURSIGHT UI TO SHOW THE RESULTS
show_ui = False

# DEV OR PROD BUCKET FOR STORING RESULTS - dev or prod
stage = 'prod'

# DISABLE FOURSIGHT TIMEOUT, True will disable  the timeout
# If the check has an internal timer, you need to modify the check besides this
disable_timeout = True

# ADD ADDITIONAL CHECK PARAMETERS YOU WANT TO USE
check_params = {} 
# To disable timeout on dedup, add time limit parameter
# check_params = {'time_limit': 100000000} 

# Run The Check
app.set_stage(stage)
apputils = app.app_utils_obj
if show_ui:
    check_params['primary'] = True
if disable_timeout:
    app.set_timeout(0)
connection = apputils.init_connection(env)
res = apputils.check_handler.run_check_or_action(connection, check, check_params)
result = json.dumps(res, indent=4)
print(result)
