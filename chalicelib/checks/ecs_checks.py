from dcicutils.ecs_utils import ECSUtils
from .helpers.confchecks import *


@check_function()
def ecs_status(connection, **kwargs):
    """ ECS Status check reports metadata on the Clusters/Services running on
        ECS in the account where foursight has been orchestrated.
    """
    check = CheckResult(connection, 'ecs_status')
    full_output = {
        'ECSMeta': {
            'clusters': {}
        }
    }
    client = ECSUtils()
    cluster_arns = client.list_ecs_clusters()
    for cluster_arn in cluster_arns:
        if 'CGAP' in cluster_arn:
            cluster_services = client.list_ecs_services(cluster_name=cluster_arn)
            full_output['ECSMeta']['clusters'][cluster_arn] = {
                'services': cluster_services
            }
    if not full_output['ECSMeta']['clusters']:
        check.status = 'WARN'
        check.summary = 'No clusters detected! Has ECS been orchestrated?'
    else:
        check.status = 'PASS'
        check.summary = 'See full output for ECS Metadata'
    check.full_output = full_output
    return check
