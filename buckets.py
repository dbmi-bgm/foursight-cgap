import boto3
import json
from chalicelib.vars import FOURSIGHT_PREFIX
from foursight_core.buckets import Buckets as Buckets_from_core

class Buckets(Buckets_from_core):
    """create and configure buckets for foursight"""

    prefix = FOURSIGHT_PREFIX
    envs = ['cgap', 'cgapdev', 'cgaptest', 'cgapwolf']


def main():
    buckets = Buckets()
    buckets.create_buckets()
    buckets.configure_env_bucket()


if __name__ == '__main__':
    main()
