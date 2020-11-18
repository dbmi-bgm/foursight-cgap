from __future__ import unicode_literals
from foursight_core.chalicelib.config import Config as _Config
from .vars import (
    FOURSIGHT_PREFIX,
)


class Config(_Config):

    prefix = FOURSIGHT_PREFIX
