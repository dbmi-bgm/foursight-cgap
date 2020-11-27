from foursight_core.decorators import Decorators
from ...vars import FOURSIGHT_PREFIX
deco = Decorators(prefix=FOURSIGHT_PREFIX)
CheckResult = deco.CheckResult
ActionResult = deco.ActionResult
check_function = deco.check_function
action_function = deco.action_function
