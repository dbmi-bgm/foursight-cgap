from chalice import Cron
from foursight_core.app_utils import app # Chalice object

@app.schedule(Cron('0/2', '*', '*', '*', '?', '*'))
def fifteen_min_checks(event):
    print('xyzzy;chalicelib_cgap;fifteen_min_checks')
    print(id(app))
    print('xyzzy;chalicelib_cgap;fifteen_min_checks;run')
    app.core.queue_scheduled_checks('all', 'fifteen_min_checks')
    print('xyzzy;chalicelib_cgap;fifteen_min_checks;run;after-run')
