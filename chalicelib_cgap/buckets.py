import boto3
import json
from chalicelib_cgap.vars import FOURSIGHT_PREFIX
from foursight_core.buckets import Buckets as Buckets_from_core


class Buckets(Buckets_from_core):
    """create and configure buckets for foursight"""

    prefix = FOURSIGHT_PREFIX
    envs = ['cgap', 'cgapdev', 'cgaptest', 'cgapwolf']

    def ff_url(self, env):
        if env == 'cgap':
            return 'https://cgap.hms.harvard.edu/'
        else:
            return 'http://%s.9wzadzju3p.us-east-1.elasticbeanstalk.com/' % self.ff_env(env)

    def es_url(self, env):
        if env == 'cgap':
            return "https://search-%s-green-6-8-vj2hurnrw7cy4hnpgds7ttklnm.us-east-1.es.amazonaws.com" % env
        else:
            return "https://search-cgap-testing-6-8-vo4mdkmkshvmyddc65ux7dtaou.us-east-1.es.amazonaws.com"


def main():
    buckets = Buckets()
    buckets.create_buckets()
    buckets.configure_env_bucket()


if __name__ == '__main__':
    main()
