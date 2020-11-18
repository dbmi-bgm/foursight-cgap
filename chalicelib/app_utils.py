from __future__ import print_function, unicode_literals
from jinja2 import Environment, FileSystemLoader, select_autoescape
from .utils import (
    basestring,
)
from .vars import (
    FOURSIGHT_PREFIX,
    HOST
)
from foursight_core.chalicelib.app_utils import AppUtils as _AppUtils
from .run_result import CheckResult, ActionResult
from .check_utils import CheckHandler
from .config import Config


class AppUtils(_AppUtils):
    
    # these must be overwritten in inherited classes
    prefix = FOURSIGHT_PREFIX
    FAVICON = 'https://cgap.hms.harvard.edu/static/img/favicon-fs.ico'
    Config = Config
    CheckHandler = CheckHandler
    CheckResult = CheckResult
    ActionResult = ActionResult
    host = HOST

    @classmethod
    def jin_env(cls):
        return Environment(
            loader=FileSystemLoader('chalicelib/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
