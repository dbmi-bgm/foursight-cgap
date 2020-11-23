from foursight_core.environment import Environment as Environment_from_core
from .vars import FOURSIGHT_PREFIX


class Environment(Environment_from_core):
    prefix = FOURSIGHT_PREFIX
