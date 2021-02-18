from conftest import *


class TestWfrChecks():
    """
    Meant for non-route utilities in chalicelib/app_utils.py
    """
    # environ = 'cgapwolf'  # Use cgapwolf for wfr tests, not DEV_ENV
    environ = 'cgap'  # Use cgap temporarily because wolf takes forever with the cgapS2status tests (DO NOT TEST ACTIONS)
    app_utils_obj = app_utils.AppUtils()
    conn = app_utils_obj.init_connection(environ)
    run = app_utils_obj.check_handler.run_check_or_action

    def test_init_connection(self):
        assert (self.conn.fs_env == self.environ)
        assert (self.conn.connections)
        assert (self.run.__name__ == 'run_check_or_action')

    def test_cgapS2_status(self):
        print("Testing cgap pipeline Part II")
        # ... make some metadata posts and patches to prep for the test
        res = self.run(self.conn, 'wfr_checks/cgapS2_status', {})
        print(res)
        assert res
        # ... assert more stuff
