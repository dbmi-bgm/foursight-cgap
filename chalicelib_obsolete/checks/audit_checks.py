from dcicutils import ff_utils
import re
import requests
from foursight_core.checks.helpers import wrangler_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


STATUS_LEVEL = {
    'released': 4,
    'archived': 4,
    'current': 4,
    'revoked': 4,
    'released to project': 3,
    'pre-release': 2,
    'restricted': 4,
    'planned': 2,
    'archived to project': 3,
    'in review by lab': 1,
    'released to lab': 1,
    'submission in progress': 1,
    'to be uploaded by workflow': 1,
    'uploading': 1,
    'uploaded': 1,
    'upload failed': 1,
    'draft': 1,
    'deleted': 0,
    'replaced': 0,
    'obsolete': 0,
}


@check_function()
def paired_end_info_consistent(connection, **kwargs):
    '''
    Check that fastqs with a paired_end number have a paired_with related_file, and vice versa
    '''
    check = CheckResult(connection, 'paired_end_info_consistent')

    search1 = 'search/?type=FileFastq&file_format.file_format=fastq&related_files.relationship_type=paired+with&paired_end=No+value'
    search2 = 'search/?type=FileFastq&file_format.file_format=fastq&related_files.relationship_type!=paired+with&paired_end%21=No+value'

    results1 = ff_utils.search_metadata(search1 + '&frame=object', key=connection.ff_keys)
    results2 = ff_utils.search_metadata(search2 + '&frame=object', key=connection.ff_keys)

    results = {'paired with file missing paired_end number':
               [result1['@id'] for result1 in results1],
               'file with paired_end number missing "paired with" related_file':
               [result2['@id'] for result2 in results2]}

    if [val for val in results.values() if val]:
        check.status = 'WARN'
        check.summary = 'Inconsistencies found in FileFastq paired end info'
        check.description = ('{} files found with a "paired with" related_file but missing a paired_end number; '
                             '{} files found with a paired_end number but missing related_file info'
                             ''.format(len(results['paired with file missing paired_end number']),
                                       len(results['file with paired_end number missing "paired with" related_file'])))
    else:
        check.status = 'PASS'
        check.summary = 'No inconsistencies in FileFastq paired end info'
        check.description = 'All paired end fastq files have both paired end number and "paired with" related_file'
    check.full_output = results
    check.brief_output = [item for val in results.values() for item in val]
    return check


@check_function()
def workflow_properties(connection, **kwargs):
    check = CheckResult(connection, 'workflow_properties')

    workflows = ff_utils.search_metadata('search/?type=Workflow&category!=provenance&frame=object',
                                         key=connection.ff_keys)
    bad = {'Duplicate Input Names in Workflow Step': [],
           'Duplicate Output Names in Workflow Step': [],
           'Duplicate Input Source Names in Workflow Step': [],
           'Duplicate Output Target Names in Workflow Step': [],
           'Missing meta.file_format property in Workflow Step Input': [],
           'Missing meta.file_format property in Workflow Step Output': []}
    by_wf = {}
    for wf in workflows:
        # print(wf['@id'])
        issues = []
        for step in wf.get('steps'):
            # no duplicates in input names
            step_inputs = step.get('inputs')
            for step_input in step_inputs:
                if (step_input['meta'].get('type') in ['data file', 'reference file'] and not
                   step_input['meta'].get('file_format')):
                    issues.append('Missing meta.file_format property in Workflow Step `{}` Input `{}`'
                                  ''.format(step.get('name'), step_input.get('name')))
            input_names = [step_input.get('name') for step_input in step_inputs]
            if len(list(set(input_names))) != len(input_names):
                issues.append('Duplicate Input Names in Workflow Step {}'.format(step.get('name')))
            # no duplicates in input source names
            sources = [(source.get('name'), source.get('step', "GLOBAL")) for
                       step_input in step_inputs for source in step_input.get('source')]
            if len(sources) != len(list(set(sources))):
                issues.append('Duplicate Input Source Names in Workflow Step {}'.format(step.get('name')))
            # no duplicates in output names
            step_outputs = step.get('outputs')
            for step_output in step_outputs:
                if (step_output['meta'].get('type') in ['data file', 'reference file'] and not
                   step_output['meta'].get('file_format')):
                    issues.append('Missing meta.file_format property in Workflow Step `{}` Output `{}`'
                                  ''.format(step.get('name'), step_output.get('name')))
            output_names = [step_output.get('name') for step_output in step_outputs]
            if len(list(set(output_names))) != len(output_names):
                issues.append('Duplicate Output Names in Workflow Step {}'.format(step.get('name')))
            # no duplicates in output target names
            targets = [(target.get('name'), target.get('step', 'GLOBAL')) for step_output in
                       step_outputs for target in step_output.get('target')]
            if len(targets) != len(list(set(targets))):
                issues.append('Duplicate Output Target Names in Workflow Step {}'.format(step.get('name')))
        if not issues:
            continue
        errors = ' '.join(issues)
        if 'Duplicate Input Names' in errors:
            bad['Duplicate Input Names in Workflow Step'].append(wf['@id'])
        if 'Duplicate Output Names' in errors:
            bad['Duplicate Output Names in Workflow Step'].append(wf['@id'])
        if 'Duplicate Input Source Names' in errors:
            bad['Duplicate Input Source Names in Workflow Step'].append(wf['@id'])
        if 'Duplicate Output Target Names' in errors:
            bad['Duplicate Output Target Names in Workflow Step'].append(wf['@id'])
        if '` Input `' in errors:
            bad['Missing meta.file_format property in Workflow Step Input'].append(wf['@id'])
        if '` Output `' in errors:
            bad['Missing meta.file_format property in Workflow Step Output'].append(wf['@id'])
        by_wf[wf['@id']] = issues

    if by_wf:
        check.status = 'WARN'
        check.summary = 'Workflows found with issues in `steps`'
        check.description = ('{} workflows found with duplicate item names or missing fields'
                             ' in `steps`'.format(len(by_wf.keys())))
    else:
        check.status = 'PASS'
        check.summary = 'No workflows with issues in `steps` field'
        check.description = ('No workflows found with duplicate item names or missing fields'
                             ' in steps property')
    check.brief_output = bad
    check.full_output = by_wf
    return check


@check_function()
def page_children_routes(connection, **kwargs):
    check = CheckResult(connection, 'page_children_routes')

    page_search = 'search/?type=Page&format=json&children.name%21=No+value'
    results = ff_utils.search_metadata(page_search, key=connection.ff_keys)
    problem_routes = {}
    for result in results:
        if result['name'] != 'resources/data-collections':
            bad_children = [child['name'] for child in result['children'] if
                            child['name'] != result['name'] + '/' + child['name'].split('/')[-1]]
            if bad_children:
                problem_routes[result['name']] = bad_children

    if problem_routes:
        check.status = 'WARN'
        check.summary = 'Pages with bad routes found'
        check.description = ('{} child pages whose route is not a direct sub-route of parent'
                             ''.format(sum([len(val) for val in problem_routes.values()])))
    else:
        check.status = 'PASS'
        check.summary = 'No pages with bad routes'
        check.description = 'All routes of child pages are a direct sub-route of parent page'
    check.full_output = problem_routes
    return check


@check_function()
def check_validation_errors(connection, **kwargs):
    '''
    Counts number of items in fourfront with schema validation errors,
    returns link to search if found.
    '''
    check = CheckResult(connection, 'check_validation_errors')

    search_url = 'search/?validation_errors.name!=No+value&type=Item'
    results = ff_utils.search_metadata(search_url + '&field=@id', key=connection.ff_keys)
    if results:
        types = {item for result in results for item in result['@type'] if item != 'Item'}
        check.status = 'WARN'
        check.summary = 'Validation errors found'
        check.description = ('{} items found with validation errors, comprising the following '
                             'item types: {}. \nFor search results see link below.'.format(
                                 len(results), ', '.join(list(types))))
        check.ff_link = connection.ff_server + search_url
    else:
        check.status = 'PASS'
        check.summary = 'No validation errors'
        check.description = 'No validation errors found.'
    return check
