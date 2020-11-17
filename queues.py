import boto3
from .chalicelib.vars import FOURSIGHT_PREFIX
from foursight_core.queues import Queues as _Queues

class Queues(_Queues):
    """create and configure queues for foursight"""
    prefix = FOURSIGHT_PREFIX


def main():
    queues = Queues()
    queues.create_queues()


if __name__ == '__main__':
    main()
