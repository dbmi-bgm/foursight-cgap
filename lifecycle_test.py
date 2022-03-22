import app

# check name
check = 'lifecycle_checks/check_metawfrs_lifecycle_status'
action = 'lifecycle_checks/patch_file_lifecycle_status'
# WHICH ENV YOU WANT TO WORK ON (data, staging, cgapwolf, ...)
env = 'cgap-wolf'
# DEV OR PROD BUCKET FOR STORING RESULTS - dev or prod
stage= 'prod'

app.set_stage(stage)
apputils = app.app_utils_obj
app.set_timeout(0)
# ADD ADDITIONAL CHECK PARAMETERS YOU WANT TO USE
check_params = {
    'metawfrs_per_run': 1
} 

connection = apputils.init_connection(env)
res = apputils.check_handler.run_check_or_action(connection, check, check_params)
result = json.dumps(res, indent=4)
print(result)