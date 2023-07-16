import re
import requests
import json
import datetime
import time
import itertools
import random
from collections import Counter
from dcicutils import ff_utils
from dcicutils.env_utils import prod_bucket_env_for_app
from foursight_core.checks.helpers import wrangler_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *
from .helpers import clone_utils


# use a random number to stagger checks
random_wait = 20


@check_function(cmp_to_last=False, action="patch_workflow_run_to_deleted")
def workflow_run_has_deleted_input_file(connection, **kwargs):
    """Checks all wfrs that are not deleted, and have deleted input files
    There is an option to compare to the last, and only report new cases (cmp_to_last)
    The full output has 2 keys, because we report provenance wfrs but not run action on them
    problematic_provenance: stores uuid of deleted file, and the wfr that is not deleted
    problematic_wfr:        stores deleted file,  wfr to be deleted, and its downstream items (qcs and output files)
    """
    check = CheckResult(connection, 'workflow_run_has_deleted_input_file')
    check.status = "PASS"
    check.action = "patch_workflow_run_to_deleted"
    my_key = connection.ff_keys
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # run the check
    search_query = 'search/?type=WorkflowRun&status!=deleted&input_files.value.status=deleted&limit=all'
    bad_wfrs = ff_utils.search_metadata(search_query, key=my_key)
    if kwargs.get('cmp_to_last', False):
        # filter out wfr uuids from last run if so desired
        prevchk = check.get_latest_result()
        if prevchk:
            prev_wfrs = prevchk.get('full_output', [])
            filtered = [b.get('uuid') for b in bad_wfrs if b.get('uuid') not in prev_wfrs]
            bad_wfrs = filtered
    if not bad_wfrs:
        check.summmary = check.description = "No live WorkflowRuns linked to deleted input Files"
        return check
    brief = str(len(bad_wfrs)) + " live WorkflowRuns linked to deleted input Files"
    # problematic_provenance stores uuid of deleted file, and the wfr that is not deleted
    # problematic_wfr stores deleted file,  wfr to be deleted, and its downstream items (qcs and output files)
    fulloutput = {'problematic_provenance': [], 'problematic_wfrs': []}
    no_of_items_to_delete = 0

    def fetch_wfr_associated(wfr_info):
        """Given wfr_uuid, find associated output files and qcs"""
        wfr_as_list = []
        wfr_as_list.append(wfr_info['uuid'])
        if wfr_info.get('output_files'):
            for o in wfr_info['output_files']:
                if o.get('value'):
                    wfr_as_list.append(o['value']['uuid'])
                if o.get('value_qc'):
                    wfr_as_list.append(o['value_qc']['uuid'])
        if wfr_info.get('output_quality_metrics'):
            for qc in wfr_info['output_quality_metrics']:
                if qc.get('value'):
                    wfr_as_list.append(qc['value']['uuid'])
        return list(set(wfr_as_list))

    for wfr in bad_wfrs:
        infiles = wfr.get('input_files', [])
        delfile = [f.get('value').get('uuid') for f in infiles if f.get('value').get('status') == 'deleted'][0]
        if wfr['display_title'].startswith('File Provenance Tracking'):
            fulloutput['problematic_provenance'].append([delfile, wfr['uuid']])
        else:
            del_list = fetch_wfr_associated(wfr)
            fulloutput['problematic_wfrs'].append([delfile, wfr['uuid'], del_list])
            no_of_items_to_delete += len(del_list)
    check.summary = "Live WorkflowRuns found linked to deleted Input Files"
    check.description = "{} live workflows were found linked to deleted input files - \
                         found {} items to delete, use action for cleanup".format(len(bad_wfrs), no_of_items_to_delete)
    if fulloutput.get('problematic_provenance'):
        brief += " ({} provenance tracking)"
    check.brief_output = brief
    check.full_output = fulloutput
    check.status = 'WARN'
    check.action_message = "Will attempt to patch %s workflow_runs with deleted inputs to status=deleted." % str(len(bad_wfrs))
    check.allow_action = True  # allows the action to be run
    return check


@action_function()
def patch_workflow_run_to_deleted(connection, **kwargs):
    action = ActionResult(connection, 'patch_workflow_run_to_deleted')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_failure': [], 'patch_success': []}
    my_key = connection.ff_keys
    for a_case in check_res['full_output']['problematic_wfrs']:
        wfruid = a_case[1]
        del_list = a_case[2]
        patch_data = {'status': 'deleted'}
        for delete_me in del_list:
            try:
                ff_utils.patch_metadata(patch_data, obj_id=delete_me, key=my_key)
            except Exception as e:
                acc_and_error = [delete_me, str(e)]
                action_logs['patch_failure'].append(acc_and_error)
            else:
                action_logs['patch_success'].append(wfruid + " - " + delete_me)
    action.output = action_logs
    action.status = 'DONE'
    if action_logs.get('patch_failure'):
        action.status = 'FAIL'
    return action


@check_function()
def item_counts_by_type(connection, **kwargs):
    def process_counts(count_str):
        # specifically formatted for FF health page
        ret = {}
        split_str = count_str.split()
        ret[split_str[0].strip(':')] = int(split_str[1])
        ret[split_str[2].strip(':')] = int(split_str[3])
        return ret

    check = CheckResult(connection, 'item_counts_by_type')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # run the check
    item_counts = {}
    warn_item_counts = {}
    req_location = ''.join([connection.ff_server, '/counts?format=json'])
    counts_res = ff_utils.authorized_request(req_location, auth=connection.ff_keys)
    if counts_res.status_code >= 400:
        check.status = 'ERROR'
        check.description = 'Error (bad status code %s) connecting to the counts endpoint at: %s.' % (counts_res.status_code, req_location)
        return check
    counts_json = json.loads(counts_res.text)
    for index in counts_json['db_es_compare']:
        counts = process_counts(counts_json['db_es_compare'][index])
        item_counts[index] = counts
        if counts['DB'] != counts['ES']:
            warn_item_counts[index] = counts
    # add ALL for total counts
    total_counts = process_counts(counts_json['db_es_total'])
    item_counts['ALL'] = total_counts
    # set fields, store result
    if not item_counts:
        check.status = 'FAIL'
        check.summary = check.description = 'Error on fourfront health page'
    elif warn_item_counts:
        check.status = 'WARN'
        check.summary = check.description = 'DB and ES item counts are not equal'
        check.brief_output = warn_item_counts
    else:
        check.status = 'PASS'
        check.summary = check.description = 'DB and ES item counts are equal'
    check.full_output = item_counts
    return check


@check_function()
def change_in_item_counts(connection, **kwargs):
    # use this check to get the comparison
    check = CheckResult(connection, 'change_in_item_counts')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    counts_check = CheckResult(connection, 'item_counts_by_type')
    latest_check = counts_check.get_primary_result()
    # get_item_counts run closest to 10 mins
    prior_check = counts_check.get_closest_result(diff_hours=24)
    if not latest_check.get('full_output') or not prior_check.get('full_output'):
        check.status = 'ERROR'
        check.description = 'There are no counts_check results to run this check with.'
        return check
    diff_counts = {}
    # drill into full_output
    latest = latest_check['full_output']
    prior = prior_check['full_output']
    # get any keys that are in prior but not latest
    prior_unique = list(set(prior.keys()) - set(latest.keys()))
    for index in latest:
        if index == 'ALL':
            continue
        if index not in prior:
            diff_counts[index] = {'DB': latest[index]['DB'], 'ES': 0}
        else:
            diff_DB = latest[index]['DB'] - prior[index]['DB']
            if diff_DB != 0:
                diff_counts[index] = {'DB': diff_DB, 'ES': 0}
    for index in prior_unique:
        diff_counts[index] = {'DB': -1 * prior[index]['DB'], 'ES': 0}

    # now do a metadata search to make sure they match
    # date_created endpoints for the FF search
    # XXX: We should revisit if we really think this search is necessary. - will 3-26-2020
    to_date = datetime.datetime.strptime(latest_check['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d+%H:%M')
    from_date = datetime.datetime.strptime(prior_check['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d+%H:%M')
    # tracking items and ontology terms must be explicitly searched for
    search_query = ''.join(['search/?type=Item&type=TrackingItem',
                            '&frame=object&date_created.from=',
                            from_date, '&date_created.to=', to_date])
    search_resp = ff_utils.search_metadata(search_query, key=connection.ff_keys)
    # add deleted/replaced items
    search_query += '&status=deleted&status=replaced'
    search_resp.extend(ff_utils.search_metadata(search_query, key=connection.ff_keys))
    for res in search_resp:

        # Stick with given type name in CamelCase since this is now what we get on the counts page
        _type = res['@type'][0]
        _entry = diff_counts.get(_type)
        if not _entry:
            diff_counts[_type] = _entry = {'DB': 0, 'ES': 0}
        if _type in diff_counts:
            _entry['ES'] += 1

    check.ff_link = ''.join([connection.ff_server, 'search/?type=Item&',
                             'type=TrackingItem&date_created.from=',
                             from_date, '&date_created.to=', to_date])
    check.brief_output = diff_counts

    # total created items from diff counts (exclude any negative counts)
    total_counts_db = sum([diff_counts[coll]['DB'] for coll in diff_counts if diff_counts[coll]['DB'] >= 0])
    # see if we have negative counts
    # allow negative counts, but make note of, for the following types
    purged_types = ['TrackingItem', 'HiglassViewConfig']
    negative_types = [tp for tp in diff_counts if (diff_counts[tp]['DB'] < 0 and tp not in purged_types)]
    inconsistent_types = [tp for tp in diff_counts if (diff_counts[tp]['DB'] != diff_counts[tp]['ES'] and tp not in purged_types)]
    if negative_types:
        negative_str = ', '.join(negative_types)
        check.status = 'FAIL'
        check.summary = 'DB counts decreased in the past day for %s' % negative_str
        check.description = ('Positive numbers represent an increase in counts. '
                             'Some DB counts have decreased!')
    elif inconsistent_types:
        check.status = 'WARN'
        check.summary = 'Change in DB counts does not match search result for new items'
        check.description = ('Positive numbers represent an increase in counts. '
                             'The change in counts does not match search result for new items.')
    else:
        check.status = 'PASS'
        check.summary = 'There are %s new items in the past day' % total_counts_db
        check.description = check.summary + '. Positive numbers represent an increase in counts.'
    check.description += ' Excluded types: %s' % ', '.join(purged_types)
    return check


@check_function(file_type=None, status=None, file_format=None, search_add_on=None, action="patch_file_size")
def identify_files_without_filesize(connection, **kwargs):
    check = CheckResult(connection, 'identify_files_without_filesize')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # must set this to be the function name of the action
    check.action = "patch_file_size"
    check.allow_action = True
    default_filetype = 'File'
    default_stati = 'released%20to%20project&status=released&status=uploaded&status=pre-release'
    filetype = kwargs.get('file_type') or default_filetype
    stati = 'status=' + (kwargs.get('status') or default_stati)
    search_query = 'search/?type={}&{}&frame=object&file_size=No value'.format(filetype, stati)
    ff = kwargs.get('file_format')
    if ff is not None:
        ff = '&file_format.file_format=' + ff
        search_query += ff
    addon = kwargs.get('search_add_on')
    if addon is not None:
        if not addon.startswith('&'):
            addon = '&' + addon
        search_query += addon
    problem_files = []
    file_hits = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=200)
    if not file_hits:
        check.allow_action = False
        check.summary = 'All files have file size'
        check.description = 'All files have file size'
        check.status = 'PASS'
        return check

    for hit in file_hits:
        hit_dict = {
            'accession': hit.get('accession'),
            'uuid': hit.get('uuid'),
            '@type': hit.get('@type'),
            'upload_key': hit.get('upload_key')
        }
        problem_files.append(hit_dict)
    check.brief_output = '{} files with no file size'.format(len(problem_files))
    check.full_output = problem_files
    check.status = 'WARN'
    check.summary = 'File metadata found without file_size'
    status_str = 'pre-release/released/released to project/uploaded'
    if kwargs.get('status'):
        status_str = kwargs.get('status')
    type_str = ''
    if kwargs.get('file_type'):
        type_str = kwargs.get('file_type') + ' '
    ff_str = ''
    if kwargs.get('file_format'):
        ff_str = kwargs.get('file_format') + ' '
    check.description = "{cnt} {type}{ff}files that are {st} don't have file_size.".format(
        cnt=len(problem_files), type=type_str, st=status_str, ff=ff_str)
    check.action_message = "Will attempt to patch file_size for %s files." % str(len(problem_files))
    check.allow_action = True  # allows the action to be run
    return check


@action_function()
def patch_file_size(connection, **kwargs):
    action = ActionResult(connection, 'patch_file_size')
    action_logs = {'s3_file_not_found': [], 'patch_failure': [], 'patch_success': []}
    # get the associated identify_files_without_filesize run result
    filesize_check_result = action.get_associated_check_result(kwargs)
    for hit in filesize_check_result.get('full_output', []):
        bucket = connection.ff_s3.outfile_bucket if 'FileProcessed' in hit['@type'] else connection.ff_s3.raw_file_bucket
        head_info = connection.ff_s3.does_key_exist(hit['upload_key'], bucket)
        if not head_info:
            action_logs['s3_file_not_found'].append(hit['accession'])
        else:
            patch_data = {'file_size': head_info['ContentLength']}
            try:
                ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], key=connection.ff_keys)
            except Exception as e:
                acc_and_error = '\n'.join([hit['accession'], str(e)])
                action_logs['patch_failure'].append(acc_and_error)
            else:
                action_logs['patch_success'].append(hit['accession'])
    action.status = 'DONE'
    action.output = action_logs
    return action


@check_function()
def validate_entrez_geneids(connection, **kwargs):
    ''' query ncbi to see if geneids are valid
    '''
    check = CheckResult(connection, 'validate_entrez_geneids')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    problems = {}
    timeouts = 0
    search_query = 'search/?type=Gene&limit=all&field=geneid'
    genes = ff_utils.search_metadata(search_query, key=connection.ff_keys)
    if not genes:
        check.status = "FAIL"
        check.description = "Could not retrieve gene records from fourfront"
        return check
    geneids = [g.get('geneid') for g in genes]

    query = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=gene&id={id}"
    for gid in geneids:
        if timeouts > 5:
            check.status = "FAIL"
            check.description = "Too many ncbi timeouts. Maybe they're down."
            return check
        gquery = query.format(id=gid)
        # make 3 attempts to query gene at ncbi
        for count in range(3):
            resp = requests.get(gquery)
            if resp.status_code == 200:
                break
            if resp.status_code == 429:
                time.sleep(0.334)
                continue
            if count == 2:
                timeouts += 1
                problems[gid] = 'ncbi timeout'
        try:
            rtxt = resp.text
        except AttributeError:
            problems[gid] = 'empty response'
        else:
            if rtxt.startswith('Error'):
                problems[gid] = 'not a valid geneid'
    if problems:
        check.summary = "{} problematic entrez gene ids.".format(len(problems))
        check.brief_output = problems
        check.description = "Problematic Gene IDs found"
        check.status = "WARN"
    else:
        check.status = "PASS"
        check.description = "GENE IDs are all valid"
    return check


def semver2int(semver):
    v = [num for num in semver.lstrip('v').split('.')]
    for i in range(1,len(v)):
        if len(v[i]) == 1:
            v[i] = '0' + v[i]
    return float(''.join([v[0] + '.'] + v[1:]))


@check_function()
def check_for_ontology_updates(connection, **kwargs):
    '''
    Checks for updates in one of the three main ontologies that the 4DN data portal uses:
    EFO, UBERON, and OBI.
    EFO: checks github repo for new releases and compares release tag. Release tag is a
    semantic version number starting with 'v'.
    OBI: checks github repo for new releases and compares release tag. Release tag is a 'v'
    plus the release date.
    UBERON: github site doesn't have official 'releases' (and website isn't properly updated),
    so checks for commits that have a commit message containing 'new release'

    If version numbers to compare against aren't specified in the UI, it will use the ones
    from the previous primary check result.
    '''
    check = CheckResult(connection, 'check_for_ontology_updates')
    check.summary = ''
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    ontologies = ff_utils.search_metadata(
        'search/?type=Ontology&frame=object',
        key=connection.ff_keys
    )
    ontologies = [o for o in ontologies if o['ontology_prefix'] != '4DN']
    versions = {
        o['ontology_prefix']: {
            'current': o.get('current_ontology_version'),
            'needs_update': False
        } for o in ontologies
    }
    for o in ontologies:
        # UBERON needs different behavior
        if o['ontology_prefix'] == 'UBERON':
            uberon = requests.get('http://svn.code.sf.net/p/obo/svn/uberon/releases/')
            ub_release = uberon._content.decode('utf-8').split('</li>\n  <li>')[-1]
            versions['UBERON']['latest'] = ub_release[ub_release.index('>') + 1: ub_release.index('</a>')].rstrip('/')
        # instead of repos etc, check download url for ontology header to get version
        elif o.get('download_url'):
            owl = requests.get(o['download_url'], headers={"Range": "bytes=0-2000"})
            if owl.status_code == 404:
                versions[o['ontology_prefix']]['latest'] = 'WARN: 404 at download_url'
                check.summary = '404 at download_url'
                check.description = 'One or more ontologies has a download_url with a 404 error.'
                check.description += ' Please update ontology item or try again later.'
                check.status = 'WARN'
                continue
            if 'versionIRI' in owl.text:
                idx = owl.text.index('versionIRI')
                vline = owl.text[idx:idx+150]
                if 'releases'in vline:
                    vline = vline.split('/')
                    v = vline[vline.index('releases')+1]
                    versions[o['ontology_prefix']]['latest'] = v
                    continue
                else:
                    # looks for date string in versionIRI line
                    match = re.search('(20)?([0-9]{2})-[0-9]{2}-(20)?[0-9]{2}', vline)
                    if match:
                        v = match.group()
                        versions[o['ontology_prefix']]['latest'] = v
                        continue
            # SO removed version info from versionIRI, use date field instead
            if 'oboInOwl:date' in owl.text:
                idx = owl.text.index('>', owl.text.index('oboInOwl:date'))
                vline = owl.text[idx+1:owl.text.index('<', idx)]
                v = vline.split()[0]
                versions[o['ontology_prefix']]['latest'] = datetime.datetime.strptime(v, '%d:%m:%Y').strftime('%Y-%m-%d')
    check.brief_output = []
    for k, v in versions.items():
        if v.get('latest') and '404' in v['latest']:
            check.brief_output.append('{} - 404'.format(k))
        elif not v['current']:
            v['needs_update'] = True
            check.brief_output.append('{} needs update'.format(k))
        elif k == 'EFO' and semver2int(v['latest']) > semver2int(v['current']):
            v['needs_update'] = True
            check.brief_output.append('{} needs update'.format(k))
        elif k != 'EFO' and v['latest'] > v['current']:
            v['needs_update'] = True
            check.brief_output.append('{} needs update'.format(k))
        else:
            check.brief_output.append('{} OK'.format(k))
    check.full_output = versions
    num = ''.join(check.brief_output).count('update')
    if '404' not in check.summary:
        if num:
            check.summary = 'Ontology updates available'
            check.description = '{} ontologies need update'.format(num)
            check.status = 'WARN'
        else:
            check.summary = 'Ontologies up-to-date'
            check.description = 'No ontology updates needed'
            check.status = 'PASS'
        if num == 1 & versions['SO']['needs_update']:
            check.status = 'PASS'
    return check


@check_function(action="patch_states_files_higlass_defaults")
def states_files_without_higlass_defaults(connection, **kwargs):
    check = CheckResult(connection, 'states_files_without_higlass_defaults')
    check.action = 'patch_states_files_higlass_defaults'
    check.full_output = {'to_add': {}, 'problematic_files': {}}
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    query = '/search/?file_type=chromatin states&type=File'
    res = ff_utils.search_metadata(query, key=connection.ff_keys)
    for re in res:
        if not re.get('higlass_defaults'):
            if not re.get('tags'):
                check.full_output['problematic_files'][re['accession']] = 'missing state tag'
            else:
                check.full_output['to_add'][re['accession']] = re["tags"]

    if check.full_output['to_add']:
        check.status = 'WARN'
        check.summary = 'Ready to patch higlass_defaults'
        check.description = 'Ready to patch higlass_defaults'
        check.allow_action = True
        check.action_message = 'Will patch higlass_defaults to %s items' % (len(check.full_output['to_add']))
    elif check.full_output['problematic_files']:
        check.status = 'WARN'
        check.summary = 'There are some files without states tags'
    else:
        check.status = 'PASS'
        check.summary = 'higlass_defaults are all set'
    return check


@action_function()
def patch_states_files_higlass_defaults(connection, **kwargs):
    action = ActionResult(connection, 'patch_states_files_higlass_defaults')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_success': [], 'patch_failure': [], 'missing_ref_file': []}
    total_patches = check_res['full_output']['to_add']

    s3 = boto3.resource('s3')
    # NOTE WELL: Omitting the appname argument in a legacy context will get the prod bucket for 'fourfront'
    #            EVEN FOR CGAP. That's maximally backward-compatible, since this used to unconditionally use
    #            the fourfront prod bucket. In an orchestrated world, the default will be better. -kmp 5-Oct-2021
    bucket = s3.Bucket('elasticbeanstalk-%s-files' % prod_bucket_env_for_app())

    query = '/search/?type=FileReference'
    all_ref_files = ff_utils.search_metadata(query, key=connection.ff_keys)
    ref_files_tags = {}
    for ref_file in all_ref_files:
        if ref_file.get('tags'):
            for ref_file_tag in ref_file.get('tags'):
                if 'states' in ref_file_tag:
                    ref_files_tags[ref_file_tag] = {'uuid': ref_file['uuid'], 'accession': ref_file['accession']}

    for item, tag in total_patches.items():
        if ref_files_tags.get(tag[0]):
            buck_obj = ref_files_tags[tag[0]]['uuid'] + '/' + ref_files_tags[tag[0]]['accession'] + '.txt'
            obj = bucket.Object(buck_obj)
            body = obj.get()['Body'].read().decode('utf8')
            lines = body.split()
            states_colors = [item for num, item in enumerate(lines) if num % 2 != 0]
            patch = {'higlass_defaults': {'colorScale': states_colors}}
            try:
                ff_utils.patch_metadata(patch, item, key=connection.ff_keys)
            except Exception as e:
                action_logs['patch_failure'].append({item: str(e)})
            else:
                action_logs['patch_success'].append(item)
        else:
            action_logs['missing_ref_file'].append({item: 'missing rows_info reference file'})

    if action_logs['patch_failure'] or action_logs['missing_ref_file']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(action="add_suggested_enum_values")
def check_suggested_enum_values(connection, **kwargs):
    """On our schemas we have have a list of suggested fields for
    suggested_enum tagged fields. A value that is not listed in this list
    can be accepted, and with this check we will find all values for
    each suggested enum field that is not in this list.
    There are 2 functions below:

    - find_suggested_enum
    This functions takes properties for a item type (taken from /profiles/)
    and goes field by field, looks for suggested enum lists, and is also
    recursive for taking care of sub-embedded objects (tagged as type=object).
    Additionally, it also takes ignored enum lists (enums which are not
    suggested, but are ignored in the subsequent search).

    * after running this function, we construct a search url for each field,
    where we exclude all values listed under suggested_enum (and ignored_enum)
    from the search: i.e. if it was FileProcessed field 'my_field' with options
    [val1, val2], url would be:
    /search/?type=FileProcessed&my_field!=val1&my_field!=val2&my_field!=No value

    - extract value
    Once we have the search result for a field, we disect it
    (again for subembbeded items or lists) to extract the field value, and =
    count occurences of each new value. (i.e. val3:10, val4:15)

    *deleted items are not considered by this check
    """
    check = CheckResult(connection, 'check_suggested_enum_values')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # must set this to be the function name of the action
    check.action = "add_suggested_enum_values"

    def find_suggested_enum(properties, parent='', is_submember=False):
        """Filter schema propteries for fields with suggested enums.
        This functions takes properties for a item type (taken from /profiles/)
        and goes field by field, looks for suggested enum lists, and is also
        recursive for taking care of sub-embedded objects (tagged as
        type=object). It also looks fore ignored enum lists.
        """
        def is_subobject(field):
            if field.get('type') == 'object':
                return True
            try:
                return field['items']['type'] == 'object'
            except:
                return False

        def dotted_field_name(field_name, parent_name=None):
            if parent_name:
                return "%s.%s" % (parent_name, field_name)
            else:
                return field_name

        def get_field_type(field):
            field_type = field.get('type', '')
            if field_type == 'string':
                if field.get('linkTo', ''):
                    return "Item:" + field.get('linkTo')
                # if multiple objects are linked by "anyOf"
                if field.get('anyOf', ''):
                    links = list(filter(None, [d.get('linkTo', '') for d in field.get('anyOf')]))
                    if links:
                        return "Item:" + ' or '.join(links)
                # if not object return string
                return 'string'
            elif field_type == 'array':
                return 'array of ' + get_field_type(field.get('items'))
            return field_type

        fields = []
        for name, props in properties.items():
            options = []
            # focus on suggested_enum ones
            if 'suggested_enum' not in str(props):
                continue
            # skip calculated
            if props.get('calculatedProperty'):
                continue
            is_array = False
            if is_subobject(props) and name != 'attachment':
                is_array = get_field_type(props).startswith('array')
                obj_props = {}
                if is_array:
                    obj_props = props['items']['properties']
                else:
                    obj_props = props['properties']
                fields.extend(find_suggested_enum(obj_props, name, is_array))
            else:
                field_name = dotted_field_name(name, parent)
                field_type = get_field_type(props)
                # check props here
                if 'suggested_enum' in props:
                    options = props['suggested_enum']
                    if 'ignored_enum' in props:
                        options.extend(props['ignored_enum'])
                # if array of string with enum
                if is_submember or field_type.startswith('array'):
                    sub_props = props.get('items', '')
                    if 'suggested_enum' in sub_props:
                        options = sub_props['suggested_enum']
                        if 'ignored_enum' in sub_props:
                            options.extend(sub_props['ignored_enum'])
                fields.append((field_name, options))
        return(fields)

    def extract_value(field_name, item, options=[]):
        """Given a json, find the values for a given field.
        Once we have the search result for a field, we disect it
        (again for subembbeded items or lists) to extract the field value(s)
        """
        # let's exclude also empty new_values
        options.append('')
        new_vals = []
        if '.' in field_name:
            part1, part2 = field_name.split('.')
            val1 = item.get(part1)
            if isinstance(val1, list):
                for an_item in val1:
                    if an_item.get(part2):
                        new_vals.append(an_item[part2])
            else:
                if val1.get(part2):
                    new_vals.append(val1[part2])
        else:
            val1 = item.get(field_name)
            if val1:
                if isinstance(val1, list):
                    new_vals.extend(val1)
                else:
                    new_vals.append(val1)
        # are these linkTo items
        if new_vals:
            if isinstance(new_vals[0], dict):
                new_vals = [i['display_title'] for i in new_vals]
        new_vals = [i for i in new_vals if i not in options]
        return new_vals

    outputs = []
    # Get Schemas
    schemas = ff_utils.get_metadata('/profiles/', key=connection.ff_keys)
    sug_en_cases = {}
    for an_item_type in schemas:
        properties = schemas[an_item_type]['properties']
        sug_en_fields = find_suggested_enum(properties)
        if sug_en_fields:
            sug_en_cases[an_item_type] = sug_en_fields

    for item_type in sug_en_cases:
        for i in sug_en_cases[item_type]:
            extension = ""
            field_name = i[0]
            field_option = i[1]
            # create queries - we might need multiple since there is a url length limit
            # Experimental - limit seems to be between 5260-5340
            # all queries are appended by filter for No value
            character_limit = 2000
            extensions = []
            extension = ''
            for case in field_option:
                if len(extension) < character_limit:
                    extension += '&' + field_name + '!=' + case
                else:
                    # time to finalize, add no value
                    extension += '&' + field_name + '!=' + 'No value'
                    extensions.append(extension)
                    # reset extension
                    extension = '&' + field_name + '!=' + case
            # add the leftover extension - there should be always one
            if extension:
                extension += '&' + field_name + '!=' + 'No value'
                extensions.append(extension)

            # only return this field
            f_ex = '&field=' + field_name

            common_responses = []
            for an_ext in extensions:
                q = "/search/?type={it}{ex}{f_ex}".format(it=item_type, ex=an_ext, f_ex=f_ex)
                responses = ff_utils.search_metadata(q, connection.ff_keys)
                # if this is the first response, assign this as the first common response
                if not common_responses:
                    common_responses = responses
                # if it is the subsequent responses, filter the commons ones with the new requests (intersection)
                else:
                    filter_ids = [i['@id'] for i in responses]
                    common_responses = [i for i in common_responses if i['@id'] in filter_ids]
                # let's check if we depleted common_responses
                if not common_responses:
                    break

            odds = []
            for response in common_responses:
                odds.extend(extract_value(field_name, response, field_option))
            if len(odds) > 0:
                outputs.append(
                    {
                        'item_type': item_type,
                        'field': field_name,
                        'new_values': dict(Counter(odds))
                    })
    if not outputs:
        check.allow_action = False
        check.brief_output = []
        check.full_output = []
        check.status = 'PASS'
        check.summary = 'No new values for suggested enum fields'
        check.description = 'No new values for suggested enum fields'
    else:
        b_out = []
        for res in outputs:
            b_out.append(res['item_type'] + ': ' + res['field'])
        check.allow_action = False
        check.brief_output = b_out
        check.full_output = outputs
        check.status = 'WARN'
        check.summary = 'Suggested enum fields have new values'
        check.description = 'Suggested enum fields have new values'
    return check


@action_function()
def add_suggested_enum_values(connection, **kwargs):
    """No action is added yet, this is a placeholder for
    automated pr that adds the new values."""
    # TODO: for linkTo items, the current values are @ids, and might need a change
    action = ActionResult(connection, 'add_suggested_enum_values')
    action_logs = {}
    # check_result = action.get_associated_check_result(kwargs)
    action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(days_back=30)
def check_external_references_uri(connection, **kwargs):
    '''
    Check if external_references.uri is missing while external_references.ref
    is present.
    '''
    check = CheckResult(connection, 'check_external_references_uri')

    days_back = kwargs.get('days_back')
    from_date_query, from_text = wrangler_utils.last_modified_from(days_back)

    search = ('search/?type=Item&external_references.ref%21=No+value' +
              '&field=external_references' + from_date_query)
    result = ff_utils.search_metadata(search, key=connection.ff_keys, is_generator=True)
    items = []
    for res in result:
        bad_refs = [er.get('ref') for er in res.get('external_references', []) if not er.get('uri')]
        if bad_refs:
            items.append({'@id': res['@id'], 'refs': bad_refs})
    names = [ref.split(':')[0] for item in items for ref in item['refs']]
    name_counts = [{na: names.count(na)} for na in set(names)]

    if items:
        check.status = 'WARN'
        check.summary = 'external_references.uri is missing'
        check.description = '%s items %sare missing uri' % (len(items), from_text)
    else:
        check.status = 'PASS'
        check.summary = 'All external_references uri are present'
        check.description = 'All dbxrefs %sare formatted properly' % from_text
    check.brief_output = name_counts
    check.full_output = items
    return check


def check_opf_lab_different_than_experiment(connection, **kwargs):
    '''
    Check if other processed files have lab (generating lab) that is different
    than the lab of that generated the experiment. In this case, the
    experimental lab needs to be added to the opf (contributing lab).
    '''
    check = CheckResult(connection, 'check_opf_lab_different_than_experiment')
    check.action = 'add_contributing_lab_opf'

    # check only recently modified files, to reduce the number of items
    days_back = kwargs.get('days_back')
    from_date_query, from_text = wrangler_utils.last_modified_from(days_back)

    search = ('search/?type=FileProcessed' +
              '&track_and_facet_info.experiment_bucket%21=No+value' +
              '&track_and_facet_info.experiment_bucket%21=processed+file' +
              '&field=experiment_sets&field=experiments' +
              '&field=lab&field=contributing_labs' + from_date_query)
    result = ff_utils.search_metadata(search, key=connection.ff_keys)

    opf = {'to_patch': [], 'problematic': []}
    exp_set_uuids = []  # Exp or ExpSet uuid list
    for res in result:
        if res.get('experiments'):
            if len(res['experiments']) != 1:  # this should not happen
                opf['problematic'].append({
                    '@id': res['@id'],
                    'experiments': [exp['uuid'] for exp in res['experiments']]})
                continue
            exp_or_set = res['experiments'][0]
        elif res.get('experiment_sets'):
            if len(res['experiment_sets']) != 1:  # this should not happen
                opf['problematic'].append({
                    '@id': res['@id'],
                    'experiment_sets': [es['uuid'] for es in res['experiment_sets']]})
                continue
            exp_or_set = res['experiment_sets'][0]
        else:  # this should not happen
            opf['problematic'].append({'@id': res['@id']})
            continue
        res['exp_set_uuid'] = exp_or_set['uuid']
        if res['exp_set_uuid'] not in exp_set_uuids:
            exp_set_uuids.append(res['exp_set_uuid'])

    # get lab of Exp/ExpSet
    result_exp_set = ff_utils.get_es_metadata(exp_set_uuids, sources=['uuid', 'properties.lab'], key=connection.ff_keys)
    uuid_2_lab = {}  # map file uuid to Exp/Set lab
    for item in result_exp_set:
        uuid_2_lab[item['uuid']] = item['properties']['lab']

    # evaluate contributing lab
    for res in result:
        if res['@id'] not in [pr['@id'] for pr in opf['problematic']]:
            contr_lab = []
            exp_set_lab = uuid_2_lab[res['exp_set_uuid']]
            if exp_set_lab == res['lab']['uuid']:
                continue
            elif res.get('contributing_labs'):
                contr_lab = [lab['uuid'] for lab in res['contributing_labs']]
                if exp_set_lab in contr_lab:
                    continue
            contr_lab.append(exp_set_lab)
            opf['to_patch'].append({
                '@id': res['@id'],
                'contributing_labs': contr_lab,
                'lab': res['lab']['display_title']})

    if opf['to_patch'] or opf['problematic']:
        check.status = 'WARN'
        check.summary = 'Supplementary files need attention'
        check.description = '%s files %sneed patching' % (len(opf['to_patch']), from_text)
        if opf['problematic']:
            check.description += ' and %s files have problems with experiments or sets' % len(opf['problematic'])
        if opf['to_patch']:
            check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'All supplementary files have correct contributing labs'
        check.description = 'All files %sare good' % from_text
    check.brief_output = {'to_patch': len(opf['to_patch']), 'problematic': len(opf['problematic'])}
    check.full_output = opf
    return check


@check_function(action="add_grouped_with_file_relation")
def grouped_with_file_relation_consistency(connection, **kwargs):
    ''' Check if "grouped with" file relationships are reciprocal and complete.
        While other types of file relationships are automatically updated on
        the related file, "grouped with" ones need to be explicitly (manually)
        patched on the related file. This check ensures that there are no
        related files that lack the reciprocal relationship, or that lack some
        of the group relationships (for groups larger than 2 files).
    '''
    check = CheckResult(connection, 'grouped_with_file_relation_consistency')
    check.action = 'add_grouped_with_file_relation'
    search = 'search/?type=File&related_files.relationship_type=grouped+with&field=related_files'
    files = ff_utils.search_metadata(search, key=connection.ff_keys, is_generator=True)

    file2all = {}  # map all existing relations
    file2grp = {}  # map "group with" existing relations
    for f in files:
        for rel in f['related_files']:
            rel_type = rel['relationship_type']
            rel_file = rel['file']['@id']
            file2all.setdefault(f['@id'], []).append(
                {"relationship_type": rel_type, "file": rel_file})
            if rel_type == "grouped with":
                file2grp.setdefault(f['@id'], []).append(rel_file)

    # list groups of related items
    groups = []
    newgroups = [set(rel).union({file}) for file, rel in file2grp.items()]

    # Check if any pair of groups in the list has a common file (intersection).
    # In that case, they are parts of the same group: merge them.
    # Repeat until all groups are disjoint (not intersecting).
    while len(groups) != len(newgroups):
        groups, newgroups = newgroups, []
        for a_group in groups:
            for each_group in newgroups:
                if not a_group.isdisjoint(each_group):
                    each_group.update(a_group)
                    break
            else:
                newgroups.append(a_group)

    # find missing relations
    missing = {}
    for a_group in newgroups:
        pairs = [(a, b) for a in a_group for b in a_group if a != b]
        for (a_file, related) in pairs:
            if related not in file2grp.get(a_file, []):
                missing.setdefault(a_file, []).append(related)

    if missing:
        # add existing relations to patch related_files
        to_patch = {}
        for f, r in missing.items():
            to_patch[f] = file2all.get(f, [])
            to_patch[f].extend([{"relationship_type": "grouped with", "file": rel_f} for rel_f in r])
        check.brief_output = missing
        check.full_output = to_patch
        check.status = 'WARN'
        check.summary = 'File relationships are missing'
        check.description = "{} files are missing 'grouped with' relationships".format(len(missing))
        check.allow_action = True
        check.action_message = ("DO NOT RUN if relations need to be removed! "
            "This action will attempt to patch {} items by adding the missing 'grouped with' relations".format(len(to_patch)))
    else:
        check.status = 'PASS'
        check.summary = check.description = "All 'grouped with' file relationships are consistent"
    return check


@action_function()
def add_grouped_with_file_relation(connection, **kwargs):
    action = ActionResult(connection, 'add_grouped_with_file_relation')
    check_res = action.get_associated_check_result(kwargs)
    files_to_patch = check_res['full_output']
    action_logs = {'patch_success': [], 'patch_failure': []}
    for a_file, related_list in files_to_patch.items():
        patch_body = {"related_files": related_list}
        try:
            ff_utils.patch_metadata(patch_body, a_file, key=connection.ff_keys)
        except Exception as e:
            action_logs['patch_failure'].append({a_file: str(e)})
        else:
            action_logs['patch_success'].append(a_file)
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(item_type=['VariantSample'], action="share_core_project")
def core_project_status(connection, **kwargs):
    """
    Ensure CGAP Core projects have their objects shared.

    Default behavior is to check only VariantSample objects, but defining
    'item_type' in check_setup.json will override the default and check status
    for all objects defined there.
    """

    check = CheckResult(connection, 'core_project_status')
    item_type = kwargs.get('item_type')
    full_output = {}
    for item in item_type:
        search_query = ('search/?project.display_title=CGAP+Core'
                        '&type=' + item +
                        '&status!=shared'
                        '&frame=object&field=uuid')
        not_shared = ff_utils.search_metadata(search_query,
                                              key=connection.ff_keys)
        if not_shared:
            not_shared_uuids = []
            for item_object in not_shared:
                not_shared_uuids.append(item_object['uuid'])
            full_output[item] = not_shared_uuids

    if full_output:
        check.status = 'WARN'
        check.summary = 'Some CGAP Core items are not shared'
        check.description = ('{} CGAP Core items do not have shared'
                             ' status'.format(sum([len(x) for x in
                                                  full_output.values()])))
        brief_output = {key: len(value) for key, value in full_output.items()}
        check.brief_output = brief_output
        check.full_output = full_output
        check.allow_action = True
        check.action = 'share_core_project'
    else:
        check.status = 'PASS'
        check.summary = 'All CGAP Core items are shared.'
        check.description = ('All CGAP Core items are shared:'
                             ' {}'.format(item_type))
    return check


@action_function()
def share_core_project(connection, **kwargs):
    """
    Change CGAP Core project item status to shared.

    Patches the status of the output of core_project_status above.
    """

    action = ActionResult(connection, 'share_core_project')
    check_response = action.get_associated_check_result(kwargs)
    check_full_output = check_response['full_output']
    # Remove FileProcessed uuids to prevent automatic patching of these items.
    if 'FileProcessed' in check_full_output:
        check_full_output.pop('FileProcessed')
    # Concatenate list of lists from full_output to single list of uuids
    uuids_to_patch = [item for sublist in check_full_output.values()
                      for item in sublist]
    action_logs = {'patch_success': [], 'patch_failure': []}
    for uuid in uuids_to_patch:
        patch_body = {'status': 'shared'}
        try:
            ff_utils.patch_metadata(patch_body, uuid, key=connection.ff_keys)
        except Exception as patch_error:
            action_logs['patch_failure'].append({uuid: str(patch_error)})
        else:
            action_logs['patch_success'].append(uuid)
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(days_back=1.02, action="queue_variants_to_update_genelist")
def update_variant_genelist(connection, **kwargs):
    """
    Searches for variant samples with genes in gene lists that are not
    currently embedded in the item, only for gene lists uploaded within a
    certain time frame (default is 1 day and ~30 minutes).

    Because of reverse link from gene to gene list, variant samples are not
    invalidated upon addition of new gene list. This check and the associated
    action search through variant samples with genes belonging to recent
    gene lists and add them to the indexing queue if the gene lists are not
    embedded.
    """

    check = CheckResult(connection, 'update_variant_genelist')
    variant_samples_to_index = []
    days_back = kwargs.get('days_back')
    current_datetime = datetime.datetime.utcnow()
    from_time = (
        current_datetime - datetime.timedelta(days=days_back)
    ).strftime("%Y-%m-%d %H:%M")
    created_search = ff_utils.search_metadata(
        'search/?type=GeneList&field=uuid&field=genes.uuid'
        '&date_created.from=' + from_time,
        key=connection.ff_keys
    )
    modified_search = ff_utils.search_metadata(
        'search/?type=GeneList&field=uuid&field=genes.uuid'
        '&last_modified.date_modified.from=' + from_time,
        key=connection.ff_keys
    )
    genelist_search = created_search
    for item in modified_search:
        if item not in genelist_search:
            genelist_search.append(item)
    for genelist in genelist_search:
        batch = []
        for idx in range(len(genelist['genes'])):
            batch.append(genelist['genes'][idx]['uuid'])
            if len(batch) == 40 or idx == (len(genelist['genes']) - 1):
                batch_terms = [
                    '&variant.genes.genes_most_severe_gene.uuid=' + uuid
                    + '&variant.genes.genes_most_severe_gene.gene_lists.uuid!='
                    + genelist['uuid']
                    for uuid in batch
                ]
                variant_sample_search_term = (
                    'search/?type=VariantSample' + ''.join(batch_terms)
                    + '&field=uuid'
                )
                variant_sample_search = ff_utils.search_metadata(
                    variant_sample_search_term,
                    key=connection.ff_keys
                )
                variant_samples_to_index += [
                    variant_sample['uuid'] for variant_sample in
                    variant_sample_search
                ]
                batch = []
    items_to_index = list(set(variant_samples_to_index))
    if items_to_index:
        check.status = 'WARN'
        check.summary = (
            'Some variant samples need to be re-indexed to reflect recently '
            'updated gene lists.'
        )
        check.description = (
            '{} variant samples need to be re-indexed to reflect recently '
            'updated gene lists.'.format(len(items_to_index))
        )
        check.full_output = items_to_index
        check.allow_action = True
        check.action = 'queue_variants_to_update_genelist'
    else:
        check.status = 'PASS'
        check.summary = (
            'All variant samples are up-to-date with recent gene lists.'
        )
        check.description = check.summary
    return check


@action_function()
def queue_variants_to_update_genelist(connection, **kwargs):
    """
    Add variant samples to indexing queue to update gene lists.

    Works with output of update_variant_genelist() above.
    """

    action = ActionResult(connection, 'update_variant_genelist')
    check_response = action.get_associated_check_result(kwargs)
    check_full_output = check_response['full_output']
    queue_index_post = {
        'uuids': check_full_output,
        'target_queue': 'primary',
        'strict': True
    }
    post_url = connection.ff_server + 'queue_indexing'
    action_logs = {'post success': [], 'post failure': []}
    try:
        post_response = ff_utils.authorized_request(
            post_url,
            auth=connection.ff_keys,
            verb='POST',
            data=json.dumps(queue_index_post)
        )
        action_logs['post success'].append(post_response.json())
    except Exception as post_error:
        action_logs['post failure'].append(str(post_error))
    if action_logs['post failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(accessions=[], version='', keep_SV_mwfr=False, create_SNV_mwfr=True, steps_to_rerun=['all'], action="clone_cases")
def get_metadata_for_cases_to_clone(connection, **kwargs):
    """
    """
    # TODO: implement steps_to_rerun, moving processed files

    check = CheckResult(connection, 'get_metadata_for_cases_to_clone')

    # get metawf_uuid
    accessions = kwargs.get('accessions')
    version = kwargs.get('version')
    steps_to_rerun = kwargs.get('steps_to_rerun')
    keep_SV_mwfr = kwargs.get('keep_SV_mwfr')
    create_SNV_mwfr = kwargs.get('create_SNV_mwfr')
    check.action = 'clone_cases'
    if not accessions:
        check.full_output = {}
        check.summary = 'No cases to clone.'
        check.description = check.summary
        check.status = 'PASS'
        return check
    if not version:
        check.full_output = {}
        check.summary = 'No pipeline version specified.'
        check.description = check.summary
        check.status = 'ERROR'
        return check
    if 'all' not in steps_to_rerun:
        check.full_output = {}
        check.summary = 'Specifying steps to rerun not yet supported. '
        check.description = ('Specifying steps to rerun not yet supported. Please rerun with '
                             'steps_to_rerun="all" and create meta-workflow run manually.')
        check.status = 'ERROR'
        return check
    meta_workflows = ff_utils.search_metadata(
        f'search/?type=MetaWorkflow&version={version}&field=version&field=name&field=uuid',
        key=connection.ff_keys
    )
    if not meta_workflows:
        check.full_output = {}
        check.summary = 'No meta-workflows found with the specified version.'
        check.description = check.summary
        check.status = 'ERROR'
        return check
    meta_workflow_dict = {mwf['name']: mwf for mwf in meta_workflows}
    output = {'run': {}, 'ignore': {}}
    for case in accessions:
        case_metadata = ff_utils.get_metadata(case, key=connection.ff_keys)
        if case_metadata.get('superseded_by'):
            output['ignore'][case] = 'This case has already been cloned.'
            continue
        mwfr = case_metadata.get('meta_workflow_run', {})
        if not mwfr:
            output['ignore'][case] = 'The case has no previous meta-workflow run. Skipping.'
            continue
        current_mwfr_version = mwfr.get('meta_workflow', {}).get('version')
        if current_mwfr_version and current_mwfr_version.upper() == version.upper():
            output['ignore'][case] = 'The case has already been run with this pipeline version.'
            continue
        updated_mwf = False
        for k, v in meta_workflow_dict.items():
            # TODO: this is a bit hacky right now, should change the mwf metadata to have title separate from version,
            # and a calcprop that combines title and version
            if k == mwfr.get('meta_workflow', {}).get('name'):
                # value is a dict so that we can add more metadata in future iterations
                output['run'][case] = {
                    'metawf_uuid': v['uuid'],
                    'create_SNV_mwfr': create_SNV_mwfr,
                    'keep_SV_mwfr': keep_SV_mwfr
                }
                updated_mwf = True
                break
        if not updated_mwf:
            output['ignore'][case] = f"{version} pipeline not found for this case's meta-workflow."
            continue

    check.full_output = output
    check.status = 'PASS'
    check.summary = f'{len(output["run"].keys())} cases ready to clone, {len(output["ignore"].keys())} cases ignored'
    check.description = check.summary
    check.allow_action = True
    return check


@action_function()
def clone_cases(connection, **kwargs):
    """
    """
    action = ActionResult(connection, 'clone_cases')
    check_response = action.get_associated_check_result(kwargs)
    clone_dict = {}
    errors = {}
    for case, data in check_response['full_output']['run'].items():
        try:
            new_case = clone_utils.CaseToClone(case, connection.ff_keys, data['metawf_uuid'],
                                               check_response['kwargs']['version'], [])
        except Exception as e:
            errors[case] = str(e)
        else:
            clone_dict.update(new_case.new_case_dict)
    action.output = {'clone success': clone_dict, 'clone fail': errors}
    if errors:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    return action
