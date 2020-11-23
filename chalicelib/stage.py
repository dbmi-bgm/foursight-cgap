from foursight_core.chalice.stage import Stage as Stage_from_core
from .vars import FOURSIGHT_PREFIX


class Stage(Stage_from_core):
    prefix = FOURSIGHT_PREFIX
