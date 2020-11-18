from foursight_core.chalicelib.sqs_utils import SQS as _SQS
from .config import Config


class SQS(_SQS):

    Config = Config
