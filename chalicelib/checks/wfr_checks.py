import json
import random
from magma_ff import run_metawfr, status_metawfr, reset_metawfr
from pipeline_utils.check_lines import *
from datetime import datetime
from dcicutils import ff_utils, s3Utils
from .helpers import wfr_utils
from .helpers.wfrset_utils import step_settings
from .helpers.wfrset_utils import lambda_limit
# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *
from .helpers.linecount_dicts import *

default_pipelines_to_run = ['WGS Trio v25', 'WGS Proband-only Cram v25', 'CNV v2', 'WES Proband-only v25']


@check_function(file_type='File', start_date=None)
def md5runCGAP_status(connection, **kwargs):
    """Searches for files that are uploaded to s3, but not went though md5 run.
    This check makes certain assumptions
    -all files that have a status<= uploaded, went through md5runCGAP
    -all files status uploading/upload failed, and no s3 file are pending,
    and skipped by this check.
    if you change status manually, it might fail to show up in this checkself.
    Keyword arguments:
    file_type -- limit search to a file type, i.e. FileFastq (default=File)
    start_date -- limit search to files generated since date  YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'md5runCGAP_status')
    my_auth = connection.ff_keys
    check.action = "md5runCGAP_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # Build the query
    query = '/search/?status=uploading&status=upload failed'
    # add file type
    f_type = kwargs.get('file_type')
    query += '&type=' + f_type
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    # if there are files, make sure they are not on s3
    no_s3_file = []
    running = []
    missing_md5 = []
    not_switched_status = []
    # multiple failed runs
    problems = []
    my_s3_util = s3Utils(env=connection.ff_env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket
    for a_file in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            check.brief_output.append('did not complete checking all')
            break
        # find bucket
        if 'FileProcessed' in a_file['@type']:
                my_bucket = out_bucket
        # elif 'FileVistrack' in a_file['@type']:
        #         my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
                my_bucket = raw_bucket
        # check if file is in s3
        file_id = a_file['accession']
        head_info = my_s3_util.does_key_exist(a_file['upload_key'], my_bucket)
        if not head_info:
            no_s3_file.append(file_id)
            continue
        md5_report = wfr_utils.get_wfr_out(a_file, "md5", key=my_auth, md_qc=True)
        if md5_report['status'] == 'running':
            running.append(file_id)
        elif md5_report['status'].startswith("no complete run, too many"):
            problems.append(file_id)
        # Most probably the trigger did not work, and we run it manually
        elif md5_report['status'] != 'complete':
            missing_md5.append(file_id)
        # There is a successful run, but status is not switched, happens when a file is reuploaded.
        elif md5_report['status'] == 'complete':
            not_switched_status.append(file_id)
    if no_s3_file:
        check.summary = 'Some files are pending upload'
        msg = str(len(no_s3_file)) + '(uploading/upload failed) files waiting for upload'
        check.brief_output.append(msg)
        check.full_output['files_pending_upload'] = no_s3_file
    if running:
        check.summary = 'Some files are running md5runCGAP'
        msg = str(len(running)) + ' files are still running md5runCGAP.'
        check.brief_output.append(msg)
        check.full_output['files_running_md5'] = running
    if problems:
        check.summary = 'Some files have problems'
        msg = str(len(problems)) + ' file(s) have problems.'
        check.brief_output.append(msg)
        check.full_output['problems'] = problems
        check.status = 'WARN'
    if missing_md5:
        check.allow_action = True
        check.summary = 'Some files are missing md5 runs'
        msg = str(len(missing_md5)) + ' file(s) lack a successful md5 run'
        check.brief_output.append(msg)
        check.full_output['files_without_md5run'] = missing_md5
        check.status = 'WARN'
    if not_switched_status:
        check.allow_action = True
        check.summary += ' Some files are have wrong status with a successful run'
        msg = str(len(not_switched_status)) + ' file(s) are have wrong status with a successful run'
        check.brief_output.append(msg)
        check.full_output['files_with_run_and_wrong_status'] = not_switched_status
        check.status = 'WARN'
    if not check.brief_output:
        check.brief_output = ['All Good!', ]
    check.summary = check.summary.strip()
    return check


@action_function(start_missing=True, start_not_switched=True)
def md5runCGAP_start(connection, **kwargs):
    """Start md5 runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'md5runCGAP_start')
    action_logs = {'runs_started': [], "runs_failed": []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    sfn = 'tibanna_zebra_' + env.replace('fourfront-', '')
    md5runCGAP_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = md5runCGAP_check_result
    targets = []
    if kwargs.get('start_missing'):
        targets.extend(md5runCGAP_check_result.get('files_without_md5run', []))
    if kwargs.get('start_not_switched'):
        targets.extend(md5runCGAP_check_result.get('files_with_run_and_wrong_status', []))
    action_logs['targets'] = targets
    for a_target in targets:
        print("processing target %s" % a_target)
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        print("getting metadata for target...")
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        print("getting attribution for target...")
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'input_file': a_file['uuid'],
                 'additional_file_parameters': {'input_file': {'mount': True}}}
        print("input template for target: %s" % str(inp_f))
        wfr_setup = step_settings('md5', 'no_organism', attributions)
        print("wfr_setup for target: %s" % str(wfr_setup))

        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, sfn)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def metawfrs_to_check_linecount(connection, **kwargs):
    """
    Find metaworkflowruns that may have completed steps for
    a linecount VCF QC line count
    """
    check = CheckResult(connection, 'metawfrs_to_check_linecount')
    my_auth = connection.ff_keys
    check.action = "line_count_test"
    check.description = "Find metaworkflow runs that need linecount qc check."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # need to build two sets of results and put them together
    # first, we want those completed MWFRs without any overall_qcs
    search_no_overall_qcs = '/search/?type=MetaWorkflowRun&overall_qcs=No+value&final_status=completed'
    result_no_overall_qcs = ff_utils.search_metadata(search_no_overall_qcs, key=my_auth)
    # second, we want those completed MWFRs with overall_qcs, but without linecount_test
    search_no_linecount_test = 'search/?type=MetaWorkflowRun&overall_qcs.name!=linecount_test&final_status=completed'
    result_no_linecount_test = ff_utils.search_metadata(search_no_linecount_test, key=my_auth)
    # add the two resulting lists together
    search_res = result_no_overall_qcs + result_no_linecount_test

    # nothing to run
    if not search_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    check.allow_action = True
    check.summary = 'Some metawfrs may have wfrs to be checked linecounts.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may have wfrs to be checked linecounts'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_run'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    return check


@action_function()
def line_count_test(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'run_metawfrs')
    action_logs = {'metawfrs_that_passed_linecount_test': [], 'metawfrs_that_failed_linecount_test': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    action_logs['error'] = []
    metawfr_uuids = check_result.get('metawfrs_to_run', {}).get('uuids', [])
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            metawfr_meta = ff_utils.get_metadata(metawfr_uuid, add_on='?frame=raw', key=my_auth)
            # we have a few different dictionaries of steps to check output from in linecount_dicts.py
            # the proband-only and family workflows have the same steps, so we assign the proband_SNV_dict
            if 'Proband-only' in metawfr_meta['title'] or 'Family' in metawfr_meta['title']:
                steps_dict = proband_SNV_dict
            # trio has novoCaller, so it has a separate dictionary of steps
            elif 'Trio' in metawfr_meta['title']:
                steps_dict = trio_SNV_dict
            # cnv/sv is a completely different pipeline, so has a many different steps
            elif 'CNV' in metawfr_meta['title']:
                steps_dict = CNV_dict
            # if this is run on something other than those expected MWFRs, we want an error.
            else:
                e = 'Unexpected MWF Title: '+metawfr_meta['title']
                action_logs['error'].append(str(e))
                continue
            # this calls check_lines from cgap-pipeline pipeline_utils check_lines.py (might get moved to generic repo in the future)
            # will return TRUE or FALSE if all pipeline steps are fine, or if there are any that do not match linecount with their partners, respectively
            linecount_result = check_lines(metawfr_uuid, my_auth, steps=steps_dict, fastqs=fastqs_dict)
            #want an empty dictionary if no overall_qcs, or a dictionary of tests and results if there are items in the overall_qcs list
            overall_qcs_dict = {qc['name']: qc['value'] for qc in metawfr_meta.get('overall_qcs', [])}
            overall_qcs_dict['linecount_test'] = 'PASS' if linecount_result else 'FAIL'
            # turn the dictionary back into a list of dictionaries that is properly structured (e.g., overall_qcs: [{"name": "linecount_test", "value": "PASS"}, {...}, {...}])
            updated_overall_qcs = [{'name': k, 'value': v} for k, v in overall_qcs_dict.items()]
            try:
                ff_utils.patch_metadata({'overall_qcs': updated_overall_qcs}, metawfr_uuid, key=my_auth)
                if linecount_result:
                    action_logs['metawfrs_that_passed_linecount_test'].append(metawfr_uuid)
                else:
                    action_logs['metawfrs_that_failed_linecount_test'].append(metawfr_uuid)
            except Exception as e:
                action_logs['error'].append(str(e))
                continue
        except Exception as e:
            action_logs['error'].append(str(e))
            continue
    action.output = action_logs
    # we want to display an error if there are any errors in the run, even if many patches are successful
    if action_logs['error'] == []:
        action.status = 'DONE'
    else:
        action.status = 'ERROR'
    return action


@check_function()
def metawfrs_to_run(connection, **kwargs):
    """Find metaworkflowruns that may need kicking
    - those with final_status pending, inactive and running.
    pending means no workflow run has started.
    inactive means some workflow runs are complete but others are pending.
    running means some workflow runs are actively running.
    """
    check = CheckResult(connection, 'metawfrs_to_run')
    my_auth = connection.ff_keys
    check.action = "run_metawfrs"
    check.description = "Find metaworkflow runs that has workflow runs to be kicked."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['pending', 'inactive', 'running', 'failed']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # nothing to run
    if not search_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    check.allow_action = True
    check.summary = 'Some metawfrs may have wfrs to be kicked.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may have wfrs to be kicked'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_run'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    return check


@action_function()
def run_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'run_metawfrs')
    action_logs = {'runs_checked_or_kicked': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    sfn = 'tibanna_zebra_' + env.replace('fourfront-', '')
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_to_run', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            run_metawfr.run_metawfr(metawfr_uuid, my_auth, verbose=True, sfn=sfn, env=env)
            action_logs['runs_checked_or_kicked'].append(metawfr_uuid)
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def metawfrs_to_checkstatus(connection, **kwargs):
    """Find metaworkflowruns that may need status-checking
    - those with final_status running.
    running means some workflow runs are actively running.
    """
    check = CheckResult(connection, 'metawfrs_to_checkstatus')
    my_auth = connection.ff_keys
    check.action = "checkstatus_metawfrs"
    check.description = "Find metaworkflow runs that has workflow runs to be status-checked."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['running']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # nothing to run
    if not search_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    check.allow_action = True
    check.summary = 'Some metawfrs may have wfrs to be status-checked.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may have wfrs to be status-checked'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    return check


@action_function()
def checkstatus_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'checkstatus_metawfrs')
    action_logs = {'runs_checked': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            status_metawfr.status_metawfr(metawfr_uuid, my_auth, verbose=True, env=env)
            action_logs['runs_checked'].append(metawfr_uuid)
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def spot_failed_metawfrs(connection, **kwargs):
    """Find metaworkflowruns that failed and reset
    if it was obviously caused by spot interruption.
    """
    check = CheckResult(connection, 'spot_failed_metawfrs')
    my_auth = connection.ff_keys
    check.action = "reset_spot_failed_metawfrs"
    check.description = "Find metaworkflow runs that has failed workflow runs that may be due to spot interruption."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['failed']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # nothing to run
    if not search_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    check.allow_action = True
    check.summary = 'Some metawfrs have failed wfrs.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may have failed wfrs'
    check.brief_output.append(msg)
    check.full_output['metawfrs_that_failed'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    if kwargs.get('reset_all_failed'):
        check.full_output['options'] = ['reset_all_failed']

    return check


@action_function()
def reset_spot_failed_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'spot_failed_metawfrs')
    action_logs = {'runs_reset': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    my_s3_util = s3Utils(env=env)
    log_bucket = my_s3_util.tibanna_output_bucket

    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_that_failed', {}).get('uuids', [])

    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            metawfr_meta = ff_utils.get_metadata(metawfr_uuid, key=my_auth, add_on='?frame=raw')
            shards_to_reset = []
            for wfr in metawfr_meta['workflow_runs']:
                if wfr['status'] == 'failed':
                    if wfr.get('workflow_run'):
                        res = ff_utils.get_metadata(wfr['workflow_run'], key=my_auth)
                    res = ff_utils.search_metadata('/search/?type=WorkflowRunAwsem&awsem_job_id=%s' % wfr['jobid'], key=my_auth)
                    if len(res) == 1:
                        res = res[0]
                    elif len(res) > 1:
                        raise Exception("multiple workflow runs for job id %s" % wfr['jobid'])
                    else:
                        raise Exception("No workflow run found for job id %s" % wfr['jobid'])
                    # If Tibanna received a spot termination notice, it will create the file JOBID.spot_failure in the
                    # Tibanna log bucket. If it failed otherwise it will throw an EC2UnintendedTerminationException
                    # which will create a corresponding entry in the workflow description
                    if my_s3_util.does_key_exist(key=wfr['jobid']+".spot_failure", bucket=log_bucket, print_error=False) or \
                       'EC2 unintended termination' in res.get('description', '') or \
                       'EC2 Idle error' in res.get('description', ''):
                        # reset spot-failed shards
                        shard_name = wfr['name'] + ':' + str(wfr['shard'])
                        shards_to_reset.append(shard_name)
            reset_metawfr.reset_shards(metawfr_uuid, shards_to_reset, my_auth, verbose=True)
            action_logs['runs_reset'].append({'metawfr': metawfr_uuid, 'shards': shards_to_reset})
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def failed_metawfrs(connection, **kwargs):
    """Find metaworkflowruns that may need status-checking
    - those with final_status running.
    running means some workflow runs are actively running.
    """
    check = CheckResult(connection, 'failed_metawfrs')
    my_auth = connection.ff_keys
    check.action = "reset_failed_metawfrs"
    check.description = "Find metaworkflow runs that has failed workflow runs."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['failed']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # nothing to run
    if not search_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    check.allow_action = True
    check.summary = 'Some metawfrs have failed wfrs.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may have failed wfrs'
    check.brief_output.append(msg)
    check.full_output['metawfrs_that_failed'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    if kwargs.get('reset_all_failed'):
        check.full_output['options'] = ['reset_all_failed']

    return check


@action_function()
def reset_failed_metawfrs(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'failed_metawfrs')
    action_logs = {'runs_reset': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_that_failed', {}).get('uuids', [])
    reset_all_failed = 'reset_all_failed' in check_result.get('options', [])  # pass ths option from check

    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            reset_metawfr.reset_failed(metawfr_uuid, my_auth, verbose=True)
            action_logs['runs_reset'].append({'metawfr': metawfr_uuid})
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def metawfrs_to_patch_samples(connection, **kwargs):
    """Find metaworkflowruns that may need samples patched with processed files
    """
    check = CheckResult(connection, 'metawfrs_to_patch_samples')
    my_auth = connection.ff_keys
    check.action = "patch_pfs_to_samples"
    check.description = "Find metaworkflow runs that may need samples to be patched."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # start with cases with a metawfr and no ingested final vcf
    query = '/search/?type=Case&meta_workflow_run!=No+value&vcf_file=No+value'
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # filter those whose samples do not have processed_files
    filtered_res = []
    for r in search_res:
        for s in r['sample_processing']['samples']:
            if len(s.get('processed_files', [])) < 2:  # bam and gvcf.
                filtered_res.append(r)
                break

    # nothing to run
    if not filtered_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['meta_workflow_run']['uuid'] for r in filtered_res]
    metawfr_titles = [r['meta_workflow_run']['display_title'] for r in filtered_res]

    check.allow_action = True
    check.summary = 'Some metawfrs may need patching samples.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may need patching samples'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    return check


@action_function()
def patch_pfs_to_samples(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'patch_pfs_to_samples')
    action_logs = {'runs_checked_for_patching': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            patch_processed_files_to_sample(metawfr_uuid, my_auth)
            action_logs['runs_checked_for_patching'].append(metawfr_uuid)
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def SNV_metawfrs_to_patch_sample_processing(connection, **kwargs):
    """Find SNV metaworkflowruns that may need sample_processing patched with processed files
    """
    check = CheckResult(connection, 'SNV_metawfrs_to_patch_sample_processing')
    my_auth = connection.ff_keys
    check.action = "patch_SNV_pfs_to_sample_processing"
    check.description = "Find SNV metaworkflow runs that may need sample processing to be patched."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # start with cases with a metawfr and no ingested final vcf
    query = '/search/?type=Case&meta_workflow_run!=No+value&vcf_file=No+value'
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # filter those whose samples do not have processed_files
    filtered_res = []
    for r in search_res:
        SNV_processed = 0
        SV_processed = 0
        result_list = r['sample_processing'].get('processed_files', [])
        for pf in result_list:
            try:
                pf['variant_type']
                if pf['variant_type'] == "SV":
                    SV_processed += 1
                elif pf['variant_type'] == "SNV":
                    SNV_processed += 1
            except:
                SNV_processed += 1
        if SNV_processed < 2:
            filtered_res.append(r)

    # nothing to run
    if not filtered_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['meta_workflow_run']['uuid'] for r in filtered_res]
    metawfr_titles = [r['meta_workflow_run']['display_title'] for r in filtered_res]

    check.allow_action = True
    check.summary = 'Some metawfrs may need patching sample processing.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may need patching sample processing'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    return check


@action_function()
def patch_SNV_pfs_to_sample_processing(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'patch_SNV_pfs_to_sample_processing')
    action_logs = {'runs_checked_for_patching': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            patch_SNV_processed_files_to_sample_processing(metawfr_uuid, my_auth)
            action_logs['runs_checked_for_patching'].append(metawfr_uuid)
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def SV_metawfrs_to_patch_sample_processing(connection, **kwargs):
    """Find SV metaworkflowruns that may need sample_processing patched with processed files
    """
    check = CheckResult(connection, 'SV_metawfrs_to_patch_sample_processing')
    my_auth = connection.ff_keys
    check.action = "patch_SV_pfs_to_sample_processing"
    check.description = "Find SV metaworkflow runs that may need sample processing to be patched."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # start with cases with a metawfr and no ingested final vcf
    query = '/search/?type=Case&meta_workflow_run_sv!=No+value&sv_vcf_file=No+value'
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # filter those whose samples do not have processed_files
    filtered_res = []
    for r in search_res:
        SNV_processed = 0
        SV_processed = 0
        result_list = r['sample_processing'].get('processed_files', [])
        for pf in result_list:
            try:
                pf['variant_type']
                if pf['variant_type'] == "SV":
                    SV_processed += 1
                elif pf['variant_type'] == "SNV":
                    SNV_processed += 1
            except:
                SNV_processed += 1
        if SV_processed < 2:
            filtered_res.append(r)

    # nothing to run
    if not filtered_res:
        check.summary = 'All Good!'
        return check

    metawfr_uuids = [r['meta_workflow_run_sv']['uuid'] for r in filtered_res]
    metawfr_titles = [r['meta_workflow_run_sv']['display_title'] for r in filtered_res]

    check.allow_action = True
    check.summary = 'Some metawfrs may need patching sample processing.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may need patching sample processing'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}
    return check


@action_function()
def patch_SV_pfs_to_sample_processing(connection, **kwargs):
    start = datetime.utcnow()
    action = ActionResult(connection, 'patch_SV_pfs_to_sample_processing')
    action_logs = {'runs_checked_for_patching': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            patch_processed_SV_files_to_sample_processing(metawfr_uuid, my_auth)
            action_logs['runs_checked_for_patching'].append(metawfr_uuid)
        except Exception as e:
            action_logs['error'] = str(e)
            break
    action.output = action_logs
    action.status = 'DONE'
    return action


def patch_processed_files_to_sample(metawfr_uuid, ff_key):
    """This currently works only for proband-only cases.
    patches samples with final bam and sample gvcf,
    patches sample_processing with vep vcf and final vcf and completed_processes."""
    metawfr_meta = ff_utils.get_metadata(metawfr_uuid, key=ff_key)
    case_acc = metawfr_meta['title'].split(' ')[-1]
    case_meta = ff_utils.get_metadata(case_acc, add_on='?frame=raw', key=ff_key)
    sp_uuid = case_meta['sample_processing']
    sp_meta = ff_utils.get_metadata(sp_uuid, key=ff_key)

    # modify this to support trio - need shard matching
    if len(sp_meta['samples']) == 1:
        final_bam = ''
        sample_gvcf = ''
        for wfr in metawfr_meta['workflow_runs']:
            if wfr['name'] == 'workflow_gatk-ApplyBQSR-check' and wfr['status'] == 'completed':
                final_bam = wfr['output'][0]['file']['uuid']
            elif wfr['name'] == 'workflow_gatk-HaplotypeCaller' and wfr['status'] == 'completed':
                sample_gvcf = wfr['output'][0]['file']['uuid']
        if final_bam and sample_gvcf:
            ff_utils.patch_metadata({'processed_files': [final_bam, sample_gvcf]}, sp_meta['samples'][0]['uuid'], key=ff_key)
    else:
        # sample name - meta mapping from sample metadata
        sample_mapping = dict()
        for sample in sp_meta['samples']:
            sample_uuid = sample['uuid']
            sample_meta = ff_utils.get_metadata(sample_uuid, add_on='?frame=raw', key=ff_key)
            sample_name = sample_meta.get('bam_sample_id', '')
            sample_mapping.update({sample_name: sample_meta})

        sample_names_arg = [inp for inp in metawfr_meta['input'] if inp['argument_name'] == 'sample_names_proband_first']
        if sample_names_arg:
            sample_names = json.loads(sample_names_arg[0]['value'])
        else:
            raise Exception("sample_names_proband_first not found in the input of metawfr %s" % metawfr_uuid)
        for i, sample_name in enumerate(sample_names):
            final_bam = ''
            sample_gvcf = ''
            for wfr in metawfr_meta['workflow_runs']:
                if wfr['name'] == 'workflow_gatk-ApplyBQSR-check' and wfr['status'] == 'completed' and wfr['shard'] == str(i):
                    final_bam = wfr['output'][0]['file']['uuid']
                elif wfr['name'] == 'workflow_gatk-HaplotypeCaller' and wfr['status'] == 'completed' and wfr['shard'] == str(i):
                    sample_gvcf = wfr['output'][0]['file']['uuid']
            if final_bam and sample_gvcf:
                pfs = sample_mapping[sample_name].get('processed_files', [])
                sample_uuid = sample_mapping[sample_name]['uuid']
                if pfs:
                    if final_bam not in pfs and sample_gvcf not in pfs:
                        raise Exception("conflicting processed files - please clean up existing processed files for sample %s" % sample_uuid)
                    elif final_bam in pfs and sample_gvcf in pfs:
                        continue  # already patched, do nothing.
                ff_utils.patch_metadata({'processed_files': [final_bam, sample_gvcf]}, sample_uuid, key=ff_key)


def patch_SNV_processed_files_to_sample_processing(metawfr_uuid, ff_key):
    """This currently works only for proband-only cases.
    patches samples with final bam and sample gvcf,
    patches sample_processing with vep vcf and final vcf and completed_processes."""
    metawfr_meta = ff_utils.get_metadata(metawfr_uuid, key=ff_key)
    case_acc = metawfr_meta['title'].split(' ')[-1]
    case_meta = ff_utils.get_metadata(case_acc, add_on='?frame=raw', key=ff_key)
    sp_uuid = case_meta['sample_processing']
    sp_meta = ff_utils.get_metadata(sp_uuid, key=ff_key)
    sp_meta_short = ff_utils.get_metadata(sp_uuid, add_on='?frame=raw', key=ff_key)

    vep_vcf = ''
    final_vcf = ''
    for wfr in metawfr_meta['workflow_runs']:
        if wfr['name'] == 'workflow_vep-annot-check' and wfr['status'] == 'completed':
            vep_vcf = wfr['output'][0]['file']['uuid']
        elif wfr['name'] == 'workflow_hg19lo_hgvsg-check' and wfr['status'] == 'completed':
            final_vcf = wfr['output'][0]['file']['uuid']
    patch_body = dict()

    #here, we aren't checking for type or replacing existing workflows because of how the check is now written
    if vep_vcf and final_vcf:
        try:
            processed_short = sp_meta_short['processed_files'] #if processed files exist (from SV), append these ones
            processed_short.append(vep_vcf)
            processed_short.append(final_vcf)
            patch_body = {'processed_files': processed_short}
        except:
            patch_body = {'processed_files': [vep_vcf, final_vcf]} #otherwise, create processed files

    if metawfr_meta['final_status'] == 'completed': #this could be turned into a function (repeated in SV as well)
        try:
            sp_meta_short['completed_processes']
            update = True
            for process in sp_meta_short['completed_processes']:
                if metawfr_meta['meta_workflow']['title'] == process:
                    update = False
            if update == True:
                process_list = sp_meta_short['completed_processes']
                process_list.append(metawfr_meta['meta_workflow']['title'])
                patch_body.update({'completed_processes': process_list})
        except:
            patch_body.update({'completed_processes': [metawfr_meta['meta_workflow']['title']]})

    if patch_body:
        ff_utils.patch_metadata(patch_body, sp_uuid, key=ff_key)


def patch_processed_SV_files_to_sample_processing(metawfr_uuid, ff_key):
    """patches sample_processing with SV full annotated vcf and higlass vcf, and completed_processes."""
    metawfr_meta = ff_utils.get_metadata(metawfr_uuid, key=ff_key)
    case_acc = metawfr_meta['title'].split(' ')[-1]
    case_meta = ff_utils.get_metadata(case_acc, add_on='?frame=raw', key=ff_key)
    sp_uuid = case_meta['sample_processing']

    final_vcf = ''
    higlass_vcf = ''

    for wfr in metawfr_meta['workflow_runs']:
        if wfr['name'] == 'workflow_SV_length_filter_vcf-check' and wfr['status'] == 'completed':
            final_vcf = wfr['output'][0]['file']['uuid']
        elif wfr['name'] == 'workflow_SV_annotation_cleaner_vcf-check' and wfr['status'] == 'completed':
            higlass_vcf = wfr['output'][0]['file']['uuid']

    patch_body = dict()
    sp_meta = ff_utils.get_metadata(sp_uuid, key=ff_key)
    sp_meta_short = ff_utils.get_metadata(sp_uuid, add_on='?frame=raw', key=ff_key)
    # if we have a final_vcf and higlass_vcf
    # we want to check if processed_files already exists
    # we need to create it with final_vcf and higlass_vcf if it doesn't
    # and if it already does:
    # we want to append if no SV type files exist
    # we want to do nothing if SV type files with same UUIDs exist
    # we want to replace if SV type files with different UUIDs exist

    if final_vcf and higlass_vcf:
        try:
            processed = sp_meta['processed_files']
            SV_type = False
            SV_uuids = []
            for file in processed:
                try:
                    if file['variant_type'] == "SV":
                        SV_type = True
                        SV_uuids.append(file['uuid'])
                except:
                    pass

            if SV_type and SV_uuids:
                update = True
                for uuid_SV in SV_uuids:
                    if uuid_SV == final_vcf:
                        update = False
                    if uuid_SV == higlass_vcf:
                        update = False
                if update == True:
                    processed_short = sp_meta_short['processed_files']
                    for uuid_SV in SV_uuids:
                        processed_short.remove(uuid_SV)
                    processed_short.append(final_vcf)
                    processed_short.append(higlass_vcf)
                    patch_body = {'processed_files': processed_short}

            else:
                processed_short = sp_meta_short['processed_files']
                processed_short.append(final_vcf)
                processed_short.append(higlass_vcf)
                patch_body = {'processed_files': processed_short}
        except:
            patch_body = {'processed_files': [final_vcf, higlass_vcf]}

    # also want to patch completed_processes
    # if it already exists, append to the list (but only if this process is not already in the list)
    # if it doesn't exist, create it with this process
    if metawfr_meta['final_status'] == 'completed':
        try:
            sp_meta_short['completed_processes']
            update = True
            for process in sp_meta_short['completed_processes']:
                if metawfr_meta['meta_workflow']['title'] == process:
                    update = False
            if update == True:
                process_list = sp_meta_short['completed_processes']
                process_list.append(metawfr_meta['meta_workflow']['title'])
                patch_body.update({'completed_processes': process_list})
        except:
            patch_body.update({'completed_processes': [metawfr_meta['meta_workflow']['title']]})

    if patch_body:
        ff_utils.patch_metadata(patch_body, sp_uuid, key=ff_key)


@check_function(start_date=None, file_accessions="")
def ingest_vcf_status(connection, **kwargs):
    """Searches for fastq files that don't have ingest_vcf
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    file_accession -- run check with given files instead of the default query
                      expects comma/space separated accessions
    """
    check = CheckResult(connection, 'ingest_vcf_status')
    my_auth = connection.ff_keys
    check.action = "ingest_vcf_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    check.allow_action = False

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # Build the query (skip to be uploaded by workflow)
    query = ("/search/?file_type=full+annotated+VCF&type=FileProcessed"
             "&file_ingestion_status=No value&file_ingestion_status=N/A"
             "&status!=uploading&status!=to be uploaded by workflow&status!=upload failed")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add accessions
    file_accessions = kwargs.get('file_accessions')
    if file_accessions:
        file_accessions = file_accessions.replace(' ', ',')
        accessions = [i.strip() for i in file_accessions.split(',') if i]
        for an_acc in accessions:
            query += '&accession={}'.format(an_acc)
    # The search
    results = ff_utils.search_metadata(query, key=my_auth)
    if not results:
        check.summary = 'All Good!'
        return check
    msg = '{} files will be added to the ingestion_queue'.format(str(len(results)))
    files = [i['uuid'] for i in results]
    check.status = 'WARN'  # maybe use warn?
    check.brief_output = [msg, ]
    check.summary = msg
    check.full_output = {'files': files,
                         'accessions': [i['accession'] for i in results]}
    check.allow_action = True
    return check


@action_function()
def ingest_vcf_start(connection, **kwargs):
    """Start ingest_vcf runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'ingest_vcf_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    ingest_vcf_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = ingest_vcf_check_result['files']
    post_body = {"uuids": targets}
    action_logs = ff_utils.post_metadata(post_body, "/queue_ingestion", my_auth)
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(limit_to_uuids="")
def long_running_wfrs_status(connection, **kwargs):
    """
    Find all runs with run status running/started. Action will cleanup their metadata, and this action might
    lead to new runs being started.
    arg:
     - limit_to_uuids: comma separated uuids to be returned to be deleted, to be used when a subset of runs needs cleanup
                       should also work if a list item is provided as input
    """
    check = CheckResult(connection, 'long_running_wfrs_status')
    my_auth = connection.ff_keys
    check.action = "long_running_wfrs_start"
    check.description = "Find runs running longer than specified, action will delete the metadata for cleanup, which might lead to re-runs by pipeline checks"
    check.brief_output = []
    check.summary = ""
    check.full_output = []
    check.status = 'PASS'
    check.allow_action = False
    # get workflow run limits
    workflow_details = wfr_utils.workflow_details
    # find all runs thats status is not complete or error
    q = '/search/?type=WorkflowRun&run_status!=complete&run_status!=error'
    running_wfrs = ff_utils.search_metadata(q, my_auth)

    # if a comma separated list of uuids is given, limit the result to them
    uuids = str(kwargs.get('limit_to_uuids'))
    if uuids:
        uuids = wfr_utils.string_to_list(uuids)
        running_wfrs = [i for i in running_wfrs if i['uuid'] in uuids]

    if not running_wfrs:
        check.summary = 'All Good!'
        return check

    print(len(running_wfrs))
    # times are UTC on the portal
    now = datetime.utcnow()
    long_running = 0

    for a_wfr in running_wfrs:
        wfr_type, time_info = a_wfr['display_title'].split(' run ')
        wfr_type_base, wfr_version = wfr_type.strip().split(' ')
        # user submitted ones use run on insteand of run
        time_info = time_info.strip('on').strip()
        try:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S')
        run_time = (now - wfr_time).total_seconds() / 3600
        run_type = wfr_type_base.strip()
        # get run_limit, if wf not found set it to an hour, we should have an entry for all runs
        run_limit = workflow_details.get(run_type, {}).get('run_time', 10)
        if run_time > run_limit:
            long_running += 1
            # find all items to be deleted
            delete_list_uuid = wfr_utils.fetch_wfr_associated(a_wfr)
            check.full_output.append({'wfr_uuid': a_wfr['uuid'],
                                      'wfr_type': run_type,
                                      'wfr_run_time': str(int(run_time)) + 'h',
                                      'wfr_run_status': a_wfr['run_status'],
                                      'wfr_status': a_wfr['status'],
                                      'items_to_delete': delete_list_uuid})
    if long_running:
        check.allow_action = True
        check.status = 'WARN'
        check.summary = "Found {} run(s) running longer than expected".format(long_running)
    else:
        check.summary = 'All Good!'
    return check


@action_function()
def long_running_wfrs_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'long_running_wfrs_start')
    my_auth = connection.ff_keys
    long_running_wfrs_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    deleted_wfrs = 0
    status_protected = 0
    for a_wfr in long_running_wfrs_check_result:
        # don't deleted if item is in protected statuses
        if a_wfr['wfr_status'] in ['shared', 'current']:
            status_protected += 1
        else:
            deleted_wfrs += 1
            for an_item_to_delete in a_wfr['items_to_delete']:
                ff_utils.patch_metadata({'status': 'deleted'}, an_item_to_delete, my_auth)
    msg = '{} wfrs were removed'.format(str(deleted_wfrs))
    if status_protected:
        msg += ', {} wfrs were skipped due to protected item status.'.format(str(status_protected))
    action.output = msg
    action.status = 'DONE'
    return action


@check_function(delete_categories='Rerun', limit_to_uuids="")
def problematic_wfrs_status(connection, **kwargs):
    """
    Find all runs with run status error. Action will cleanup their metadata, and this action might
    lead to new runs being started.
    arg:
     - delete_category: comma separated category list
                        which categories to delete with action, by default Rerun is deleted
     - limit_to_uuids: comma separated uuids to be returned to be deleted, to be used when a subset of runs needs cleanup
                       should also work if a list item is provided as input
    """
    check = CheckResult(connection, 'problematic_wfrs_status')
    my_auth = connection.ff_keys
    check.action = "problematic_wfrs_start"
    check.description = "Find errored runs, action will delete the metadata for cleanup, which might lead to re-runs by pipeline checks"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'report_only': [], 'cleanup': []}
    check.status = 'PASS'
    check.allow_action = False
    # find all runs thats status is not complete or error
    q = '/search/?type=WorkflowRun&run_status=error'
    errored_wfrs = ff_utils.search_metadata(q, my_auth)
    # if a comma separated list of uuids is given, limit the result to them
    uuids = str(kwargs.get('limit_to_uuids'))
    if uuids:
        uuids = wfr_utils.string_to_list(uuids)
        errored_wfrs = [i for i in errored_wfrs if i['uuid'] in uuids]

    delete_categories = str(kwargs.get('delete_categories'))
    if delete_categories:
        delete_categories = wfr_utils.string_to_list(delete_categories)

    if not errored_wfrs:
        check.summary = 'All Good!'
        return check
    print(len(errored_wfrs))
    # report wfrs with error with warning
    check.status = 'WARN'
    # categorize errored runs based on the description keywords
    category_dictionary = {'NotEnoughSpace': 'Not enough space',
                           'Rerun': 'rerun',
                           'CheckLog': 'tibanna log --',
                           'EC2Idle': 'EC2 Idle',
                           'PatchError': 'Bad status code for PATCH',
                           'NotCategorized': ''  # to record all not categorized
                           }
    # counter for categories
    counter = {k: 0 for k in category_dictionary}
    # if a delete_category is not in category_dictionary, bail
    wrong_category = [i for i in delete_categories if i not in category_dictionary]
    if wrong_category:
        check.summary = 'Category was not found: {}'.format(wrong_category)
        return check

    for a_wfr in errored_wfrs:
        wfr_type, time_info = a_wfr['display_title'].split(' run ')
        wfr_type_base, wfr_version = wfr_type.strip().split(' ')
        run_type = wfr_type_base.strip()
        # categorize
        desc = a_wfr.get('description', '')
        category = ''
        for a_key in category_dictionary:
            if category_dictionary[a_key] in desc:
                counter[a_key] += 1
                category = a_key
                break
        # all should be assigned to a category
        assert category
        # find all items to be deleted
        delete_list_uuid = wfr_utils.fetch_wfr_associated(a_wfr)

        info_pack = {'wfr_uuid': a_wfr['uuid'],
                     'wfr_type': run_type,
                     'wfr_run_status': a_wfr['run_status'],
                     'wfr_status': a_wfr['status'],
                     'wfr_description': a_wfr.get('description', '')[:50],
                     'category': category,
                     'items_to_delete': delete_list_uuid}
        action_category = ''
        # based on the category, place it in one of the lists in full output
        if category in delete_categories:
            action_category = 'To be deleted'
            check.full_output['cleanup'].append(info_pack)
        else:
            check.full_output['report_only'].append(info_pack)
            action_category = 'Only Reported'
        # add a short description for brief output
        check.brief_output.append("{}, {}, {}, {}".format(a_wfr['uuid'],
                                                          run_type,
                                                          category,
                                                          action_category
                                                          ))

    if check.full_output['cleanup']:
        check.allow_action = True

    report_catories = [i for i in category_dictionary if i not in delete_categories]
    check.summary = "{} wfrs ({}) will be deleted, and {} wfrs ({}) are reported".format(
        sum([counter[i] for i in delete_categories]),
        ",".join([i for i in delete_categories if counter[i]]),
        sum([counter[i] for i in report_catories]),
        ",".join([i for i in report_catories if counter[i]])
    )
    # add summary as the first item in brief output
    check.brief_output.insert(0, check.summary)
    return check


@action_function()
def problematic_wfrs_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'problematic_wfrs_start')
    my_auth = connection.ff_keys
    problematic_wfrs_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    deleted_wfrs = 0
    status_protected = 0
    for a_wfr in problematic_wfrs_check_result['cleanup']:
        # don't deleted if item is in protected statuses
        if a_wfr['wfr_status'] in ['shared', 'current']:
            status_protected += 1
        else:
            deleted_wfrs += 1
            for an_item_to_delete in a_wfr['items_to_delete']:
                ff_utils.patch_metadata({'status': 'deleted'}, an_item_to_delete, my_auth)
    msg = '{} wfrs were removed'.format(str(deleted_wfrs))
    if status_protected:
        msg += ', {} wfrs were skipped due to protected item status.'.format(str(status_protected))
    action.output = msg
    action.status = 'DONE'
    return action
