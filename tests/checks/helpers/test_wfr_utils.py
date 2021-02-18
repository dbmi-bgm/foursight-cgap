from chalicelib.checks.helpers.wfr_utils import *
import pytest
import time
import re


class TestWfrUtils():

    environ = 'fourfront-cgapwolf'
    my_auth = ff_utils.get_authentication_with_server(ff_env=environ)

    # work in progress

    def test_create_metadata_for_case(self):
        res = create_metadata_for_case(self.my_auth)
        assert isinstance(res, dict)
        assert len(res['file_fastq']) == 6  # default trio x 2 (PE)
        cleanup_metadata_for_case(res['case'][0]['uuid'], self.my_auth,
                                  rm_individual=True, cleanup=True)  # clean up afterwards

    def test_cleanup_metadata_for_case_already_deleted(self):
        # existing but deleted entries
        case_uuids_to_cleanup = ['54f63c6d-d36b-4d89-adb9-84df20df0109',
                                 'c551b74d-a6f2-49b3-b919-e1ff174135a0',
                                 '9c5f6354-bd39-4e12-95d1-37335e7e46c1',
                                 '14504b75-5c26-4488-92ea-635e57967da0',
                                 'f0700398-3ae3-456a-88ad-d55022f2ad97',
                                 'a2efd5e0-156d-4fb9-87d0-cc1ed358d1db',
                                 '7ad9d314-a304-4a62-96f3-fcded9382f53',
                                 '253e4fc7-0f46-4b73-8ae0-75c98be31355']
        for uuid in case_uuids_to_cleanup:
            res = cleanup_metadata_for_case(uuid, self.my_auth)
            assert res is None

    def test_cleanup_metadata_for_case_nonexistent(self):
        res = cleanup_metadata_for_case('aksfvlweif', self.my_auth)  # nonexistend entry
        assert res is None

    def test_cleanup_metadata_for_case(self):
        res = create_metadata_for_case(self.my_auth)  # use a case created on the fly just for this test
        case_uuid_to_cleanup = res['case'][0]['uuid']
        res = cleanup_metadata_for_case(case_uuid_to_cleanup, self.my_auth)
        #assert 'workflow_run_awsem' in res.keys()
        assert 'individual' not in res.keys()
        res = cleanup_metadata_for_case(case_uuid_to_cleanup, self.my_auth, rm_individual=True)
        assert 'individual' in res.keys()
        # actually clean up
        while(True):
            try:
                res2 = cleanup_metadata_for_case(case_uuid_to_cleanup, self.my_auth,
                                                 rm_individual=True, cleanup=True)
                break
            except Exception as e:
                if 'source_experiments' in str(e):
                    s = str(e.value) 
                    s = re.sub('.+.elasticbeanstalk.com/', '', s)
                    uuid = re.sub(': 422.+', '', s)
                    delete_source_experiments(uuid, self.my_auth)
        time.sleep(60)  # wait 1min for indexing
        for itemtype in res:
            for uuid in res[itemtype]:
                meta = ff_utils.get_metadata(uuid, self.my_auth)
                assert meta['status'] == 'deleted'



def prep_metadata_for_put(uuid, key):
    res = ff_utils.get_metadata(uuid, key=key,  add_on='?frame=raw')
    if 'source_experiments' in res:
        del res['source_experiments']
    del res['file_classification']
    del res['schema_version']
    del res['last_modified']
    for ex in res.get('extra_files', []):
        del ex['upload_key']
        del ex['href']
    return res


def delete_source_experiments(uuid, key):
    res = prep_metadata_for_put(uuid, key)
    put_metadata(res, res['uuid'], key=key, ff_env='fourfront-cgapwolf')


def put_metadata(put_item, obj_id='', key=None, ff_env=None, add_on=''):
    '''
    Patch metadata given the patch body and an optional obj_id (if not provided,
    will attempt to use accession or uuid from patch_item body).
    Either takes a dictionary form authentication (MUST include 'server')
    or a string fourfront-environment.
    '''
    auth = ff_utils.get_authentication_with_server(key, ff_env)
    obj_id = obj_id if obj_id else put_item.get('accession', put_item.get('uuid'))
    if not obj_id:
        raise Exception("ERROR getting id from given object %s for the request to"
                        " patch item. Supply a uuid or accession." % obj_id)
    put_url = '/'.join([auth['server'], obj_id]) + ff_utils.process_add_on(add_on)
    # format item to json
    put_item = json.dumps(put_item)
    response = ff_utils.authorized_request(put_url, auth=auth, verb='PUT', data=put_item)
    return ff_utils.get_response_json(response)
