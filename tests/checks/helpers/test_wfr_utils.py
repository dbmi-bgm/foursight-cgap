from chalicelib.checks.helpers.wfr_utils import *


class TestWfrUtils():

    environ = 'fourfront-cgapwolf'
    my_auth = ff_utils.get_authentication_with_server(ff_env=environ)

    # work in progress
    def test_init_connection(self):
        case_uuid_to_cleanup = 'c551b74d-a6f2-49b3-b919-e1ff174135a0'
        res = cleanup(case_uuid_to_cleanup, self.my_auth)
        assert res

