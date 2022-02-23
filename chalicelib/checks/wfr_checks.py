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


default_pipelines_to_run = ['CNV Germline v1', 'WGS Trio v26',
    'WGS Proband-only Cram v26', 'WES Proband-only v26', 'WES Family v26', 'WES Trio v26',
    'WGS Proband-only v26', 'WGS Family v26', 'WGS Trio v27', 'WGS Proband-only Cram v27',
    'WES Proband-only v27', 'WES Family v27', 'WES Trio v27', 'WGS Proband-only v27',
    'WGS Family v27', 'SV Germline v3', 'WGS Upstream GATK Proband v27']



#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
#           !!! FUNCTIONS USED BY CHECKS !!!
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#                   General functions
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
############################################################
# _metadata_for_patch
############################################################
def _metadata_for_patch(metawfr_uuid, ff_key):
    """
    Fetch meta data for metaworkflowrun (metawfr_meta)
    and sample_processing (sp_meta)
    """
    ### Getting metawfr metadata
    metawfr_meta = ff_utils.get_metadata(metawfr_uuid, key=ff_key)
    ### Getting the case that will be patched
    case_acc = metawfr_meta['title'].split(' ')[-1]
    case_meta = ff_utils.get_metadata(case_acc, add_on='frame=raw', key=ff_key)
    ### Getting the sample_processing that will be modified and used for patching
    sp_uuid = case_meta['sample_processing']
    sp_meta = ff_utils.get_metadata(sp_uuid, key=ff_key)

    return metawfr_meta, sp_meta

############################################################
# _sp_meta_processed_files
############################################################
def _sp_meta_processed_files(sp_meta):
    """
    List uuids for processed_files in sample_processing if any
    else return an empty list
    """
    processed_files = []
    for pf_dict in sp_meta.get('processed_files', []):
        processed_files.append(pf_dict['uuid'])

    return processed_files

############################################################
# _eval_qcs
############################################################
def _eval_qcs(metawfr_meta):
    """
    Check final_status for metawfr_meta is completed
    Check expected QCs in overall_qcs are PASS
    """
    ### Check final_status
    if metawfr_meta['final_status'] != 'completed':
        return False

    ### Check overall_qcs
    # check is overall_qcs
    overall_qcs = metawfr_meta.get('overall_qcs', [])
    if not overall_qcs: # check if overall_qcs exists
        return False

    # check expected qcs are in overall_qcs
    expected_qcs = {'linecount_test'}
    available_qcs = set()
    for qc in overall_qcs:
        available_qcs.add(qc['name'])

    if expected_qcs.difference(available_qcs):
        return False

    # check all expected qcs are pass
    for qc in overall_qcs:
        if qc['name'] in expected_qcs and qc['value'] != 'PASS':
            return False

    return True

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#               Check specific functions
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
############################################################
# patch_processed_files_to_sample (upstream processed files)
############################################################
def patch_processed_files_to_sample(metawfr_uuid, final_steps, ff_key):
    """
    Patch sample processed_files,
        patch the output file from each step in final_steps
    does not patch completed_processes
    """
    ### Getting metadata
    metawfr_meta, sp_meta = _metadata_for_patch(metawfr_uuid, ff_key)
    ### Checking QCs are PASS and final_status completed
    if not _eval_qcs(metawfr_meta): # if check fails, return
        return 'failed final_status or QCs validation'

    ### Getting uuid for output files and patch
    if len(sp_meta['samples']) == 1: # proband only
        final_files = []
        # get uuids for output files
        for wfr in metawfr_meta['workflow_runs']:
            if wfr['name'] in final_steps:
                final_files.append(wfr['output'][0]['file']['uuid'])
        # patch sample
        ff_utils.patch_metadata({'processed_files': final_files}, sp_meta['samples'][0]['uuid'], key=ff_key)
    else: # trio
        # sample name
        #   meta mapping from sample metadata
        sample_mapping = dict()
        for sample in sp_meta['samples']:
            sample_uuid = sample['uuid']
            sample_meta = ff_utils.get_metadata(sample_uuid, add_on='frame=raw', key=ff_key)
            sample_name = sample_meta.get('bam_sample_id', '')
            sample_mapping.update({sample_name: sample_meta})

        sample_names_arg = [inp for inp in metawfr_meta['input'] if inp['argument_name'] == 'sample_names_proband_first']
        # sample_names_proband_first is an argument from the metaworkflowrun, probably there is a better way to run
        #   this patching but for now we can keep this as it is working
        if sample_names_arg:
            sample_names = json.loads(sample_names_arg[0]['value'])
        else:
            raise Exception("sample_names_proband_first not found in the input of metawfr %s" % metawfr_uuid)
        for i, sample_name in enumerate(sample_names):
            final_files = []
            # get uuids for output files
            for wfr in metawfr_meta['workflow_runs']:
                if wfr['name'] in final_steps and wfr['shard'] == str(i):
                    final_files.append(wfr['output'][0]['file']['uuid'])
            # get sample uuid
            sample_uuid = sample_mapping[sample_name]['uuid']
            ## !!!
            #  the check is not robust enough for now and we may have a situation
            #    where for a case with multiple samples only some of the sample are patched.
            #    The samples not patched will trigger this action that will work on all samples
            #    no matter what, we want to skip here samples that are already patched
            # #
            pfs = sample_mapping[sample_name].get('processed_files', [])
            if pfs:
                if final_files != pfs:
                    raise Exception("conflicting processed files - please clean up existing processed files for sample %s" % sample_uuid)
                else:
                    continue  # already patched, do nothing.
            # patch sample
            ff_utils.patch_metadata({'processed_files': final_files}, sample_uuid, key=ff_key)

    return 'patched succesfully'

############################################################
# patch_processed_files_to_sample_processing
############################################################
def patch_processed_files_to_sample_processing(metawfr_uuid, final_steps, ff_key):
    """
    Patch sample_processing processed_files,
        patch the output file from each step in final_steps

    Patch sample_processing completed_processes,
        patch the metaworkflow title
    """
    ### Getting metadata
    metawfr_meta, sp_meta = _metadata_for_patch(metawfr_uuid, ff_key)
    ### Checking QCs are PASS and final_status completed
    if not _eval_qcs(metawfr_meta): # if check fails, return
        return 'failed final_status or QCs validation'

    ### Getting uuid for files to patch and create patch body
    patch_body, final_files = dict(), []
    # get uuids for output files
    for wfr in metawfr_meta['workflow_runs']:
        if wfr['name'] in final_steps:
            final_files.append(wfr['output'][0]['file']['uuid'])

    # update patch_body processed_files
    # this is not checking processed_files makes sense
    #   that should be done at the check
    processed_files = _sp_meta_processed_files(sp_meta)
    processed_files += final_files
    patch_body = {'processed_files': processed_files}

    # update patch_body completed_process
    completed_processes = sp_meta.get('completed_processes', [])
    meta_workflow_title = metawfr_meta['meta_workflow']['title']
    if meta_workflow_title not in completed_processes:
        completed_processes.append(meta_workflow_title)
        patch_body.update({'completed_processes': completed_processes})

    ### Patching metadata
    if patch_body:
        ff_utils.patch_metadata(patch_body, sp_meta['uuid'], key=ff_key)

    return 'patched succesfully'



#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
#                !!! CHECKS & ACTIONS !!!
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
############################################################
# md5runCGAP_status
############################################################
@check_function(file_type='File', start_date=None)
def md5runCGAP_status(connection, **kwargs):
    """
    Searches for files that are uploaded to s3, but not went through md5 run
    Check assumptions:
        - all files that have a status uploaded run through md5runCGAP
        - all files status uploading/upload failed and NO s3 file are skipped

    If the status is changed manually, it might fail to show up in this checkself

    kwargs:
        file_type -- limit search to a file type, i.e. FileFastq (default=File)
        start_date -- limit search to files generated since date  YYYY-MM-DD
        run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    ### General check attributes
    start = datetime.utcnow()
    check = CheckResult(connection, 'md5runCGAP_status')
    my_auth = connection.ff_keys
    check.action = "md5runCGAP_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # basic query
    query = '/search/?status=uploading&status=upload failed'
    # add file type
    f_type = kwargs.get('file_type')
    query += '&type=' + f_type
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    res = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run, return
    if not res:
        check.summary = 'All Good!'
        return check

    # else
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
        # most probably the trigger did not work, and we run it manually
        elif md5_report['status'] != 'complete':
            missing_md5.append(file_id)
        # there is a successful run, but status is not switched, happens when a file is reuploaded.
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
    """
    Start md5 runs by sending compiled input_json to run_workflow endpoint
    """
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


############################################################
# metawfrs_to_check_linecount
############################################################
@check_function()
def metawfrs_to_check_linecount(connection, **kwargs):
    """
    Find 'completed' metaworkflowruns
    Run a line_count_test qc check
    """
    ### General check attributes
    check = CheckResult(connection, 'metawfrs_to_check_linecount')
    my_auth = connection.ff_keys
    check.action = "line_count_test"
    check.description = "Find metaworkflow runs that need linecount qc check."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # need to query two sets of results and put them together
    # first, we want those completed MWFRs without any overall_qcs
    search_no_overall_qcs = '/search/?type=MetaWorkflowRun&overall_qcs=No+value&final_status=completed'
    result_no_overall_qcs = ff_utils.search_metadata(search_no_overall_qcs, key=my_auth)
    # second, we want those completed MWFRs with overall_qcs, but without linecount_test
    search_no_linecount_test = 'search/?type=MetaWorkflowRun&overall_qcs.name!=linecount_test&final_status=completed'
    result_no_linecount_test = ff_utils.search_metadata(search_no_linecount_test, key=my_auth)
    # add the two resulting lists together
    search_res = result_no_overall_qcs + result_no_linecount_test

    # if nothing to run, return
    if not search_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    ### More check attributes, setting up the action
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
    action = ActionResult(connection, 'line_count_test')
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
            metawfr_meta = ff_utils.get_metadata(metawfr_uuid, add_on='frame=raw', key=my_auth)
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


############################################################
# metawfrs_to_run
############################################################
@check_function()
def metawfrs_to_run(connection, **kwargs):
    """
    Find metaworkflowruns that may need to run steps
    - those with final_status 'pending', 'inactive' and 'running'

    'pending' means no workflow run has started
    'inactive' means some workflow runs are completed but others are pending
    'running' means some workflow runs are actively running

    note: the check is currently looking also for 'failed' metaworkflowruns,
          we probably want to disable this
    """
    ### General check attributes
    check = CheckResult(connection, 'metawfrs_to_run')
    my_auth = connection.ff_keys
    check.action = "run_metawfrs"
    check.description = "Find metaworkflow runs that has workflow runs to be kicked."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # query
    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['pending', 'inactive', 'running', 'failed']])
            # this is currently looking also for failed MWFRs, we probably want to disable this
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run, return
    if not search_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    ### More check attributes, setting up the action
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


############################################################
# metawfrs_to_checkstatus
############################################################
@check_function()
def metawfrs_to_checkstatus(connection, **kwargs):
    """
    Find metaworkflowruns that may need status-checking
    - those with final_status 'running'

    'running' means some workflow runs are actively running
    """
    ### General check attributes
    check = CheckResult(connection, 'metawfrs_to_checkstatus')
    my_auth = connection.ff_keys
    check.action = "checkstatus_metawfrs"
    check.description = "Find metaworkflow runs that has workflow runs to be status-checked."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # query
    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['running']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run, return
    if not search_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    ### More check attributes, setting up the action
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

############################################################
# spot_failed_metawfrs
############################################################
@check_function()
def spot_failed_metawfrs(connection, **kwargs):
    """
    Find metaworkflowruns that failed
    - those with status 'failed'

    Reset status to pending if it is spot interruption
    """
    ### General check attributes
    check = CheckResult(connection, 'spot_failed_metawfrs')
    my_auth = connection.ff_keys
    check.action = "reset_spot_failed_metawfrs"
    check.description = "Find metaworkflow runs that has failed workflow runs that may be due to spot interruption."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # query
    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['failed']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run, return
    if not search_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    ### More check attributes, setting up the action
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
    action = ActionResult(connection, 'reset_spot_failed_metawfrs')
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
            metawfr_meta = ff_utils.get_metadata(metawfr_uuid, key=my_auth, add_on='frame=raw')
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

############################################################
# failed_metawfrs
############################################################
@check_function()
def failed_metawfrs(connection, **kwargs):
    """
    Find metaworkflowruns that are failed
    - those with status 'failed'

    Reset status to pending all
    """
    ### General check attributes
    check = CheckResult(connection, 'failed_metawfrs')
    my_auth = connection.ff_keys
    check.action = "reset_failed_metawfrs"
    check.description = "Find metaworkflow runs that has failed workflow runs."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    # Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # query
    query = '/search/?type=MetaWorkflowRun' + \
            ''.join(['&final_status=' + st for st in ['failed']])
    query += ''.join(['&meta_workflow.title=' + mwf for mwf in default_pipelines_to_run])
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run, return
    if not search_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['uuid'] for r in search_res]
    metawfr_titles = [r['title'] for r in search_res]

    ### More check attributes, setting up the action
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
    action = ActionResult(connection, 'reset_failed_metawfrs')
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


############################################################
# metawfrs_to_patch_samples
############################################################
@check_function()
def metawfrs_to_patch_samples(connection, **kwargs):
    """
    Find metaworkflowruns that need to patch processed files to samples
    - bam and gvcf
    """
    ### General check attributes
    check = CheckResult(connection, 'metawfrs_to_patch_samples')
    my_auth = connection.ff_keys
    check.action = "patch_pfs_to_samples"
    check.description = "Find metaworkflow runs that may need samples to be patched."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # query cases with a metawfr and no ingested final vcf for SNV
    #   final vcf for SNV it is associated to vcf_file in the metadata
    query = '/search/?type=Case&meta_workflow_run!=No+value&vcf_file=No+value' # !!! THIS QUERY IS NOT ROBUST
    search_res = ff_utils.search_metadata(query, key=my_auth)

    # filter those whose samples do not have processed_files
    filtered_res = []
    for r in search_res:
        for s in r['sample_processing']['samples']:
            if len(s.get('processed_files', [])) < 2:  # 2 because there are 2 processed_files when it's patched already
                                                       #     and so we want to skip that case, bam and gvcf
                filtered_res.append(r)
                break

    # if nothing to run, return
    if not filtered_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['meta_workflow_run']['uuid'] for r in filtered_res]
    metawfr_titles = [r['meta_workflow_run']['display_title'] for r in filtered_res]

    ### More check attributes, setting up the action
    check.allow_action = True
    check.summary = 'Some metawfrs may need patching samples.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may need patching samples'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}

    return check

@action_function()
def patch_pfs_to_samples(connection, **kwargs):
    ### General action attributes
    start = datetime.utcnow()
    action = ActionResult(connection, 'patch_pfs_to_samples')
    action_logs = {'runs_checked_for_patching': [], 'runs_checked_for_patching_log': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    action_logs['error'] = []
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.

    ### Running the action
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            # action function call
            res = patch_processed_files_to_sample(metawfr_uuid,
                ['workflow_gatk-ApplyBQSR-check', 'workflow_gatk-HaplotypeCaller'],
                my_auth)
            action_logs['runs_checked_for_patching'].append(metawfr_uuid)
            action_logs['runs_checked_for_patching_log'].append(metawfr_uuid + ' ' + res)
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


############################################################
# SNV_metawfrs_to_patch_sample_processing
############################################################
@check_function()
def SNV_metawfrs_to_patch_sample_processing(connection, **kwargs):
    """
    Find SNV metaworkflowruns that need to patch processed files to sample_processing
    - vep_vcf and final_vcf
    """
    ### General check attributes
    check = CheckResult(connection, 'SNV_metawfrs_to_patch_sample_processing')
    my_auth = connection.ff_keys
    check.action = "patch_SNV_pfs_to_sample_processing"
    check.description = "Find SNV metaworkflow runs that may need sample processing to be patched."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # query cases with a metawfr and no ingested final vcf for SNV
    #   final vcf for SNV it is associated to vcf_file in the metadata
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
            except Exception: # SNV probably should be specified and not just be the default
                SNV_processed += 1
        if SNV_processed < 2: # 2 because there are 2 processed_files when it's patched already
                              #     and so we want to skip that case
            filtered_res.append(r)

    # if nothing to run, return
    if not filtered_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['meta_workflow_run']['uuid'] for r in filtered_res]
    metawfr_titles = [r['meta_workflow_run']['display_title'] for r in filtered_res]

    ### More check attributes, setting up the action
    check.allow_action = True
    check.summary = 'Some metawfrs may need patching sample processing.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may need patching sample processing'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}

    return check

@action_function()
def patch_SNV_pfs_to_sample_processing(connection, **kwargs):
    ### General action attributes
    start = datetime.utcnow()
    action = ActionResult(connection, 'patch_SNV_pfs_to_sample_processing')
    action_logs = {'runs_checked_for_patching': [], 'runs_checked_for_patching_log': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    action_logs['error'] = []
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.

    ### Running the action
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            # action function call
            res = patch_processed_files_to_sample_processing(metawfr_uuid,
                ['workflow_vep-annot-check', 'workflow_hg19lo_hgvsg-check'],
                my_auth)
            action_logs['runs_checked_for_patching'].append(metawfr_uuid)
            action_logs['runs_checked_for_patching_log'].append(metawfr_uuid + ' ' + res)
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


############################################################
# SV_metawfrs_to_patch_sample_processing
############################################################
@check_function()
def SV_metawfrs_to_patch_sample_processing(connection, **kwargs):
    """
    Find SV metaworkflowruns that need to patch processed files to sample_processing
    - final_vcf and higlass_vcf
    """
    ### General check attributes
    check = CheckResult(connection, 'SV_metawfrs_to_patch_sample_processing')
    my_auth = connection.ff_keys
    check.action = "patch_SV_pfs_to_sample_processing"
    check.description = "Find SV metaworkflow runs that may need sample processing to be patched."
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # start with cases with a metawfr and no ingested final vcf for SV
    #   final vcf for SV it is associated to structural_variant_vcf_file in the metadata
    query = '/search/?type=Case&meta_workflow_run_sv!=No+value&structural_variant_vcf_file=No+value'
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
            except Exception:
                SNV_processed += 1
        if SV_processed < 2: # 2 because there are 2 processed_files when it's patched already
                             #     and so we want to skip that case
            filtered_res.append(r)

    # if nothing to run
    if not filtered_res:
        check.summary = 'All Good!'
        return check

    # else
    metawfr_uuids = [r['meta_workflow_run_sv']['uuid'] for r in filtered_res]
    metawfr_titles = [r['meta_workflow_run_sv']['display_title'] for r in filtered_res]

    ### More check attributes, setting up the action
    check.allow_action = True
    check.summary = 'Some metawfrs may need patching sample processing.'
    check.status = 'WARN'
    msg = str(len(metawfr_uuids)) + ' metawfrs may need patching sample processing'
    check.brief_output.append(msg)
    check.full_output['metawfrs_to_check'] = {'titles': metawfr_titles, 'uuids': metawfr_uuids}

    return check

@action_function()
def patch_SV_pfs_to_sample_processing(connection, **kwargs):
    ### General action attributes
    start = datetime.utcnow()
    action = ActionResult(connection, 'patch_SV_pfs_to_sample_processing')
    action_logs = {'runs_checked_for_patching': [], 'runs_checked_for_patching_log': []}
    my_auth = connection.ff_keys
    env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = check_result
    action_logs['error'] = []
    metawfr_uuids = check_result.get('metawfrs_to_check', {}).get('uuids', [])
    random.shuffle(metawfr_uuids)  # if always the same order, we may never get to the later ones.

    ### Running the action
    for metawfr_uuid in metawfr_uuids:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        try:
            # action function call
            res = patch_processed_files_to_sample_processing(metawfr_uuid,
                ['workflow_SV_length_filter_vcf-check', 'workflow_SV_annotation_cleaner_vcf-check'],
                my_auth)
            action_logs['runs_checked_for_patching'].append(metawfr_uuid)
            action_logs['runs_checked_for_patching_log'].append(metawfr_uuid + ' ' + res)
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


############################################################
# ingest_vcf_status
############################################################
@check_function(start_date=None, file_accessions="")
def ingest_vcf_status(connection, **kwargs):
    """
    Search for full annotated VCF files that needs to be ingested

    kwargs:
        start_date -- limit search to files generated since a date formatted YYYY-MM-DD
        file_accession -- run check with given files instead of the default query
                          expects comma/space separated accessions
    """
    ### General check attributes
    check = CheckResult(connection, 'ingest_vcf_status')
    my_auth = connection.ff_keys
    check.action = "ingest_vcf_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    check.allow_action = False

    ### Check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    ### Check
    # basic query (skip to be uploaded by workflow)
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

    results = ff_utils.search_metadata(query, key=my_auth)

    # if nothing to run
    if not results:
        check.summary = 'All Good!'
        return check

    # else
    msg = '{} files will be added to the ingestion_queue'.format(str(len(results)))
    files = [i['uuid'] for i in results]

    ### More check attributes, setting up the action
    check.status = 'WARN'  # maybe use warn?
    check.brief_output = [msg, ]
    check.summary = msg
    check.full_output = {'files': files,
                         'accessions': [i['accession'] for i in results]}
    check.allow_action = True

    return check

@action_function()
def ingest_vcf_start(connection, **kwargs):
    """
    Start ingest_vcf runs by sending compiled input_json to run_workflow endpoint
    """
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


@check_function(file_accessions="")
def check_vcf_ingestion_errors(connection, **kwargs):
    """
    Check for finding full annotated VCFs that have failed ingestion, so that they
    can be reset and the ingestion rerun if needed.
    """
    check = CheckResult(connection, 'check_vcf_ingestion_errors')
    accessions = [accession.strip() for accession in kwargs.get('file_accessions', '').split(',') if accession]
    ingestion_error_search = 'search/?file_type=full+annotated+VCF&type=FileProcessed&file_ingestion_status=Error'
    if accessions:
        ingestion_error_search += '&accession='
        ingestion_error_search += '&accession='.join(accessions)
    ingestion_error_search += '&field=@id&field=file_ingestion_error'
    results = ff_utils.search_metadata(ingestion_error_search, key=connection.ff_keys)
    output = {}
    for result in results:
        if len(result.get('file_ingestion_error')) > 0:
            # usually there are 100 errors, so just report first error, user can view item to see others
            output[result['@id']] = result['file_ingestion_error'][0].get('body')
    check.full_output = output
    check.brief_output = list(output.keys())
    if output:
        check.status = 'WARN'
        check.summary = f'{len(check.brief_output)} VCFs failed ingestion'
        check.description = check.summary
        check.allow_action = True
        check.action = ''
    else:
        check.status = 'PASS'
        check.summary = 'No VCFs found with ingestion errors'
        check.description = check.summary
    return check


@action_function()
def reset_vcf_ingestion_errors(connection, **kwargs):
    """
    Takes VCFs with ingestion errors, patches file_ingestion_status to 'N/A', and
    removes file_ingestion_error property. This will allow ingestion to be retried.
    """
    action = ActionResult(connection, 'reset_vcf_ingestion_errors')
    check_result_vcfs = action.get_associated_check_result(kwargs).get('brief_output', [])
    action_logs = {'success': [], 'fail': {}}
    for vcf in check_result_vcfs:
        patch = {'file_ingestion_status': 'N/A'}
        try:
            resp = ff_utils.patch_metadata(patch, vcf + '?delete_fields=file_ingestion_error', key=connection.ff_keys)
        except Exception as e:
            action_logs['fail'][vcf] = str(e)
        else:
            if resp['status'] == 'success':
                action_logs['success'].append(vcf)
            else:
                action_logs['fail'][vcf] = resp['status']
    action.output = action_logs
    if action_logs['fail']:
        action.status = 'ERROR'
    else:
        action.status = 'DONE'
    return action


############################################################
# long_running_wfrs_status
# !!!! THIS CHECK HAS NOT BEEN UPDATED IN A LONG TIME
#           IT'S NOT DOING ANYTHING RIGHT NOW !!!!
# ---> need to update wfr_utils.workflow_details
############################################################
@check_function(limit_to_uuids="")
def long_running_wfrs_status(connection, **kwargs):
    """
    Find all runs with run status 'running' or 'started'
    Return long running ones, over a time limit
    The action will cleanup the associated metadata
    Cleaning the metadata might lead to new runs being started

    kwargs:
        limit_to_uuids -- comma separated uuids to return for delition
                          to be used when a subset of runs needs cleanup
                          should also work if a list item is provided as input
    """
    ### General check attributes
    check = CheckResult(connection, 'long_running_wfrs_status')
    my_auth = connection.ff_keys
    check.action = "long_running_wfrs_start"
    check.description = "Find runs running longer than specified, action will delete the metadata for cleanup, which might lead to re-runs by pipeline checks"
    check.brief_output = []
    check.summary = ""
    check.full_output = []
    check.status = 'PASS'
    check.allow_action = False

    ### Get workflow run limits
    workflow_details = wfr_utils.workflow_details

    ### Query
    # find all runs with status not complete or error
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
    """
    Start runs by sending compiled input_json to run_workflow endpoint
    """
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


############################################################
# problematic_wfrs_status
############################################################s
@check_function(delete_categories='Rerun', limit_to_uuids="")
def problematic_wfrs_status(connection, **kwargs):
    """
    Find all runs with run status 'error'
    The action will cleanup the associated metadata
    Cleaning the metadata might lead to new runs being started

    kwargs:
        delete_category -- comma separated list of categories to delete,
                           by default Rerun is deleted
        limit_to_uuids -- comma separated list of uuids to return for deletion
                          to be used when a subset of runs needs cleanup
                          should also work if a list item is provided as input
    """
    ### General check attributes
    check = CheckResult(connection, 'problematic_wfrs_status')
    my_auth = connection.ff_keys
    check.action = "problematic_wfrs_start"
    check.description = "Find errored runs, action will delete the metadata for cleanup, which might lead to re-runs by pipeline checks"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'report_only': [], 'cleanup': []}
    check.status = 'PASS'
    check.allow_action = False

    ### Query
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
    """
    Start runs by sending compiled input_json to run_workflow endpoint
    """
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




"""
Redoing things for MWFRs that describe how/where/what to patch incoming files.

Overall flow:
    - Find MWFRs that have recently finished (?) and haven't had their
        files patched (will need prop on MWFR (?) to indicate file patch status)
    - Ensure such MWFRs have passed all QCs
        - If not, make note and don't patch any files (?)
    - Search through output processed files and collect those with patching instructions
        - FileProcessed schema will need additional fields for patching instructions
            - What exactly is needed here? Probably choice of Sample, SampleProcessing,
                probably a dict for future flexibility
        - MWF custom_pf_fields will need update with strict enum for this field
            so MWFR with typos invalidate
    - Generic patch function to patch files to appropriate location
        - Can try to validate all such patches and then proceed
            - If something doesn't validate, make note and stop (?)
            - If patch fails after validation, make note and continue (?)
    - Update MWFR file patch status property to success/fail/etc.

May want to add in another check to do something if a patch failed on a MWFR
even though it validated?

Other checks/actions required?
"""

def make_embed_request(ids=None, fields=None, connection=None):
    """"""
    post_body = {"ids": ids, "fields": fields}
    result = ff_utils.authorized_request(
        "/embed", verb="POST", auth=connection.ff_key, data=json.dumps(post_body)
    )
    return result


QC_PASS = "PASS"


def nested_getter(item, fields):
    """"""
    if not fields:
        return None
    field_to_get = fields.pop(0)
    if isinstance(item, dict):
        result = item.get(field_to_get)
    elif isinstance(item, list):
        result = []
        for sub_item in item:
            result.append(nested_getter(sub_item, [field_to_get]))
    else:
        result = None
    if fields:
        result = nested_getter(result, fields)
    if isinstance(result, list):
        if len(result) == 1:
            result = result[0]
    return result


def evaluate_meta_workflow_run_qcs(meta_workflow_run_metadata, connection=None):
    """"""
    overall_qcs = meta_workflow_run_metadata.get("overall_qcs", [])
    for overall_qc in overall_qcs:
        overall_qc_value = overall_qc.get("value")
        if overall_qc_value != QC_PASS:
            return False
    uuid = meta_workflow_run_metadata.get("uuid")
    embed_field = (
        "workflow_runs.workflow_run.output_files.value_qc.overall_quality_status"
    )
    post_response = make_embed_request(
        ids=[uuid], fields=[embed_field], connection=connection
    )
    for result in post_response:
        qc_status_fields = nested_getter(result, embed_field.split("."))
        if qc_status_fields:
            for qc_status in qc_status_fields:
                if qc_status != QC_PASS:
                    return False
    return True


@check_function()
def find_successful_metaworkflowruns(connection, **kwargs):
    """"""
    check = CheckResult(connection, "find_successful_metaworkflowruns")
    check.description = ""
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = "PASS"
    check.action = ""
    check.allow_action = True
    check.action_message = "Add MetaWorkflowRuns' output files to their cases."

    query = (
        "/search/?type=MetaWorkflowRun&final_status=completed"
        "&output_files_linked=No+value"
    )
    search = ff_utils.search_metadata(query, key=connection.ff_keys)
    if not search:
        msg = (
            "Found no completed MetaWorkflowRuns with output files to add to their"
            " respective cases."
        )
        check.brief_output.append(msg)
    meta_workflow_runs_failed_qc = []
    meta_workflow_runs_passed_qc = []
    for meta_workflow_run in search:
        uuid = meta_workflow_run.get("uuid")
        qc_check = evaluate_meta_workflow_run_qcs(meta_workflow_run)
        if qc_check is False:
            meta_workflow_runs_failed_qc.append(uuid)
        else:
            meta_workflow_runs_passed_qc.append(uuid)
    if meta_workflow_runs_failed_qc:
        check.status = "WARN"
        msg = (
            "%s MetaWorkflowRuns have failed QC checks and won't have output files"
            " added to their respective cases."
            % len(meta_workflow_runs_failed_qc)
        )
        check.brief_output.append(msg)
        check.full_output["failed_qc"] = meta_workflow_runs_failed_qc
    if meta_workflow_runs_passed_qc:
        msg = (
            "%s MetaWorkflowRuns have passed QC checks and will have output files"
            " added to their respective cases."
            % len(meta_workflow_runs_failed_qc)
        )
        check.brief_output.append(msg)
        check.full_output["passed_qc"] = meta_workflow_runs_passed_qc
    return check


def update_meta_workflow_run_files_linked(
    meta_workflow_run_uuid, errors=None, connection=None
):
    """"""
    if not errors:
        patch_body = {"output_files_linked_status": "success"}
    else:
        patch_body = {
            "output_files_linked_status": "error",
            "output_files_linked_errors": errors
        }
    ff_utils.patch_metadata(patch_body, meta_workflow_run_uuid, key=connection.ff_key)


#def update_dictionary_value_list(dictionary, key, value):
#    """"""
#    if key in dictionary:
#        dictionary[key].append(value)
#    else:
#        dictionary[key] = [value]
#
#
#def get_output_file_link_locations(output_file_metadata, link_locations, link_errors):
#    """"""
#    file_uuid = output_file_metadata.get("uuid")
#    case_accession = output_file_metadata.get("associated_case")
#    linktos_to_make = output_file_metadata.get("linkto_placement")
#    if linktos_to_make and not case_accession:
#        error_msg = "Missing case information"
#        update_dictionary_value_list(link_errors, file_uuid, error_msg)
#    elif linktos_to_make:
#        for linkto_to_make in linktos_to_make:
#            file_linkto_location = linkto_to_make.get("location")
#            if not file_linkto_location:
#                continue
#            update_dictionary_value_list(
#                link_locations, file_linkto_location, file_uuid
#            )


# def make_file_linkto(file_uuid, item_to_link, field_to_link, link_type, connection=None):
#     """Assuming field_to_link is top-level on item_to_link."""
#     linkto_error = None
#     item_metadata = ff_utils.get(item_to_link, add_on="frame=raw&datastore=database", key=connection.ff_key)
#     item_field_to_link = item_metadata.get(field_to_link)
#     item_type = item_metadata.get("@type")[0]
#     item_schema = ff_utils.get("/profiles/" + item_type + ".json",
#             key=connection.ff_key)
#     item_properties = item_schema.get("properties", {})
#     field_properties = item_properties.get(field_to_link, {})
#     field_type = field_properties.get("type")
#     # Perhaps check that field is a linkTo as well???
#     if field_type == "array":
#         if item_field_to_link and file_uuid not in item_field_to_link:
#             item_field_to_link.append(file_uuid)
#             patch_body = {field_to_link: item_field_to_link}
#             ff_utils.patch_metadata(patch_body, item_to_link, key=connection.ff_key)
#     elif field_type == "string" and item_field_to_link != file_uuid:
#         patch_body = {field_to_link: file_uuid}
#         ff_utils.patch_metadata(patch_body, item_to_link, key=connection.ff_key)
#     else:
#         linkto_error = "Something"
#     return linkto_error
# 
# 
# def create_file_linktos(files_to_link, connection=None):
#     """"""
#     # TODO: Make patch to file linkTo
#     files_with_link_errors = set()
#     for file_uuid, linkto_placement_metadata in files_to_link.items():
#         for linkto_placement in linkto_placement_metadata:
#             errors = []
#             linkto_error = None
#             item_to_link = linkto_placement.get("item")
#             if not item_to_link:
#                 errors.append("Missing \"item\" field")
#             field_to_link = linkto_placement.get("field")
#             if not field_to_link:
#                 errors.append("Missing \"field\" field")
#             link_type = linkto_placement.get("type")
#             if not link_type:
#                 errors.append("Missing \"type\" field")
#             if not errors:
#                 linkto_error = make_file_linkto(file_uuid, item_to_link, field_to_link, link_type,
#                     connection=connection)
#             if linkto_error:
#                 errors.append(linkto_error)
#             if errors:
#                 files_with_link_errors.add(file_uuid)
#     return list(files_with_link_errors)


def get_sample_mapping(meta_workflow_run_uuid, connection=None):
    """"""
    result = {}
    meta_workflow_run = ff_utils.get_metadata(
        meta_workflow_run_uuid, addon="?frame=raw"
    )
    associated_case = meta_workflow_run.get("associated_case")
    meta_workflow_run_input = meta_workflow_run.get("input", [])
    ordered_sample_ids = []
    for item in meta_workflow_run_input:
        argument_name = item.get("argument_name")
        if argument_name == "sample_names_proband_first":
            ordered_sample_ids = json.loads(item.get("value"))
            break
    case = ff_utils.get_metadata(associated_case, addon="?frame=raw")
    sample_processing = case.get("sample_processing", {})
    sample_processing_uuid = sample_processing.get("uuid")
    result["SampleProcessing"] = sample_processing_uuid
    ordered_sample_uuids = [x for x in ordered_sample_ids]
    samples = sample_processing.get("samples", [])
    for sample in samples:
        sample_uuid = sample.get("uuid")
        sample_id = sample.get("bam_sample_id")
        if sample_id in ordered_sample_ids:
            idx = ordered_sample_ids.index(sample_id)
            ordered_sample_uuids[idx] = sample_uuid
    result["Sample"] = ordered_sample_uuids
    return result


def create_file_linktos(output_files_to_link, sample_mapping, connection=None):
    """"""
    failed_file_linktos = []
    to_patch = {}
    for file_uuid, linkto_type in output_files_to_link.items():
        linkto_locations = linkto_type.get("locations")
        if "Sample" in linkto_locations:
            sample_idx = linkto_type.get("shard")
            sample_uuid = sample_mapping.get("Sample")[sample_idx]
            add_to_patch(to_patch, sample_uuid, file_uuid)
        if "SampleProcessing" in linkto_locations:
            sample_processing_uuid = sample_mapping.get("SampleProcessing")
            add_to_patch(to_patch, sample_processing_uuid, file_uuid)
    for item_to_patch, files_to_patch in to_patch.items():
        need_to_patch = False
        item = ff_utils.get_metadata(
            item_to_patch, connection=connection, addon="?frame=raw"
        )
        item_processed_files = item.get("processed_files", [])
        for processed_file in files_to_patch:
            if processed_file not in item_processed_files:
                need_to_patch = True
                item_processed_files.append(processed_file)
        if need_to_patch:
            patch_body = {"processed_files": item_processed_files}
            patch_response = ff_utils.patch_metadata(
                item_to_patch, patch_body, connection=connection
            )
            if patch_response.get("status") != "success":
                failed_file_linktos += files_to_patch
    return failed_file_linktos


def add_to_dict_as_list(dictionary, key, value):
    """"""
    existing_item_value = dictionary.get(key)
    if existing_item_value:
        existing_item_value.append(value)
    else:
        dictionary[key] = [value]


@action_function()
def move_meta_workflow_run_output_files_to_case(connection, **kwargs):
    """"""
    action = ActionResult(connection, "")
    actions.status = "FAIL"
    action.output = {"successful": [], "errored": []}

    file_linkto_field = "linkto_location"

    check_results = action.get_associated_check_result(kwargs)
    meta_workflow_run_uuids = check_results.get("passed_qc", [])
    for meta_workflow_run_uuid in meta_workflow_run_uuids:
        output_files_to_link = {}
        embed_fields = [
            "workflow_runs.output.shard",
            "workflow_runs.output.file.uuid",
            "worflow_runs.output.file." + file_linkto_field + ".*",
        ]
        embed_request = make_embed_request(
            ids=[meta_workflow_run_uuid], fields=embed_fields, connection=connection
        )
        for embed_result in embed_request:
            output = embed_result.get("workflow_runs", {}).get("output", [])
            for item in output:
                output_file = item.get("file", {})
                output_file_uuid = output_file.get("uuid")
                output_file_linkto_field = output_file.get(file_linkto_field)
                if not output_file_linkto_field:
                    continue
                shard = item.get("shard")
                if shard:
                    shard = int(shard[0])  # Used as index for sample mapping
                file_fields = {"locations": output_file_linkto_field, "shard": shard}
                output_files_to_link[output_file_uuid] = file_fields
        if output_files_to_link:
            sample_mapping = get_sample_mapping(
                meta_workflow_run_uuid, connection=connection
            )
            link_errors = create_file_linktos(
                output_files_to_link, sample_mapping, connection=connection
            )
        update_meta_workflow_run_files_linked(
            meta_workflow_run_uuid, errors=link_errors, connection=connection
        )
        if link_errors:
            action.output["errored"].append(meta_workflow_run_uuid)
        else:
            action.output["successful"].append(meta_workflow_run_uuid)
    action.status = "DONE"
    return action
