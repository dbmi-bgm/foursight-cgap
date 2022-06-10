from chalicelib.checks.helpers.lifecycle_utils import get_file_lifecycle_status, \
    check_file_lifecycle_status, STANDARD, INFREQUENT_ACCESS, GLACIER, DEEP_ARCHIVE, DELETED
import json, datetime
from unittest.mock import patch

# TO RUN THESE TESTS LOCALLY USE: pytest --noconftest

class TestLifecycleChecks():

    files = []
    projects = []

    # Pytest does not discover classes with __init__. Therefore this workaround to load the data
    def load_metadata(self):  
        # load it only once
        if len(self.files) > 0:
            return

        with open('lifecycle_testdata.json', 'r') as f:
            data = json.load(f)
            self.files = data["files"]
            self.projects = data["projects"]
            
    def search_metadata_mock_func(self, path, key):
        # The check calls this function twice. Just return [] the second time
        if "s3_lifecycle_last_checked=No+value" in path:
            return []
        return self.files

    def get_metadata_mock_func(self, project_uuid, key):
        return next(filter(lambda x: x["uuid"] == project_uuid, self.projects))

    # Fix the utcnow() function so that we get consistent results
    def get_datetime_utcnow_mock_func(self):
        return datetime.datetime(2022, 5, 24)


    @patch('chalicelib.checks.helpers.lifecycle_utils.get_datetime_utcnow')
    @patch('dcicutils.ff_utils.get_metadata')
    @patch('dcicutils.ff_utils.search_metadata')
    def test_check_file_lifecycle_status(self, mock_search_metadata, mock_get_metadata, mock_datetime_utcnow):
        self.load_metadata()
        mock_search_metadata.side_effect = self.search_metadata_mock_func 
        mock_get_metadata.side_effect = self.get_metadata_mock_func 
        mock_datetime_utcnow.side_effect = self.get_datetime_utcnow_mock_func 
        # None of the input arguments have actually any effect, as they all go into the search_metadata query, which is mocked
        check_result = check_file_lifecycle_status(1, 1, 1, None)
 
        assert check_result['status'] == "PASS"

        expected_lifecycle_statuses = {
            "file_1": DEEP_ARCHIVE, # Default lifecycle policy
            "file_2": DELETED,
            "file_3": DELETED,
            "file_4": INFREQUENT_ACCESS,
            "file_5": DEEP_ARCHIVE,
            "file_6": DELETED,
            "file_7": DEEP_ARCHIVE,
            "file_8": INFREQUENT_ACCESS,
            "file_9": DELETED,
            "file_10": DELETED, # project specific lifecycle policy
            "file_20": DEEP_ARCHIVE,
            "file_21": DEEP_ARCHIVE,
            "file_22": DELETED,
            "file_23": INFREQUENT_ACCESS,
            "file_24": DEEP_ARCHIVE,
            "file_25": DEEP_ARCHIVE, # status does not change, should not be in the result set
            "file_26": STANDARD, # status does not change, should not be in the result set
            "file_27": INFREQUENT_ACCESS,
            "file_28": DELETED,
            "file_40": INFREQUENT_ACCESS, # extra files
            "file_41": INFREQUENT_ACCESS, # extra files
            "file_50": INFREQUENT_ACCESS, # custom policy
            "file_51": GLACIER, # custom policy
            "file_52": DEEP_ARCHIVE, # custom policy
            "file_53": DELETED, # custom policy
        }

        files_to_update = check_result["files_to_update"]
        for files in files_to_update:
            uuid = files["uuid"]
            assert files["new_lifecycle_status"] == expected_lifecycle_statuses[uuid], f'Assertion error for file {uuid}'

        # Verify that 25 and 26 are not there
        files_without_update = check_result["files_without_update"]
        assert "file_25" in files_without_update
        assert "file_26" in files_without_update

        # Make sure extra files are handled correctly
        file_40_extra = next(filter(lambda x: x["uuid"] == "file_40" and x["is_extra_file"], files_to_update))
        assert file_40_extra["upload_key"] == "file_40_upload_key_extra"
        assert file_40_extra["new_lifecycle_status"] == INFREQUENT_ACCESS

        file_41 = next(filter(lambda x: x["uuid"] == "file_41" and not x["is_extra_file"], files_to_update))
        assert file_41["upload_key"] == "file_41_upload_key"
        assert file_41["new_lifecycle_status"] == INFREQUENT_ACCESS
        files_41_extra = list(filter(lambda x: x["uuid"] == "file_41" and x["is_extra_file"], files_to_update))
        assert len(files_41_extra) == 2

        # Test files with incorrect metadata
        self.files[0]["s3_lifecycle_category"] = "invalid"
        check_result = check_file_lifecycle_status(1, 1, 1, None)
        assert check_result["status"] == "WARN"
        assert check_result["files_with_issues"][0] == "file_1"
        assert len(check_result["files_to_update"]) > 1

        # new lifecycle status of the followng case would be infrequent access, but it is already in deep archive
        self.files[0]["s3_lifecycle_category"] = "short_term_access_long_term_archive"
        self.files[0]["s3_lifecycle_status"] = "deep archive"
        check_result = check_file_lifecycle_status(1, 1, 1, None)
        assert check_result["status"] == "WARN"
        assert check_result["files_with_issues"][0] == "file_1"
        assert "Unsupported storage class transition" in check_result["warning"]
        assert len(check_result["files_to_update"]) > 1
        #assert 1==2

