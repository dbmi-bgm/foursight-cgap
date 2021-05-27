from dcicutils.ecs_utils import ECSUtils
from dcicutils.cloudformation_utils import get_ecr_repo_url
from dcicutils.docker_utils import DockerUtils
from dcicutils.ecr_utils import ECRUtils
from .deployment_checks import clone_repo_to_temporary_dir
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


@check_function()
def update_ecs_application_versions(connection, cluster_name=None, **kwargs):
    """ This check is intended to be run AFTER the user has finished pushing
        the relevant images to ECR. Triggers an update on all services for
        the CGAP cluster. If no cluster_name is passed, Foursight will infer
        one if there is only a single option - otherwise error is raised.

        Note that this check just kicks the process - it does not block until
        the cluster update has finished.
    """
    check = CheckResult(connection, 'update_ecs_application_versions')
    client = ECSUtils()
    cluster_arns = client.list_ecs_clusters()
    if not cluster_name:
        cgap_candidate = list(filter(lambda arn: 'CGAP' in arn, cluster_arns))
        if len(cgap_candidate) > 1:
            check.status = 'FAIL'
            check.summary = 'Ambiguous cluster setup (not proceeding): %s' % cgap_candidate
        else:
            client.update_all_services(cluster_name=cgap_candidate[0])
            check.status = 'PASS'
            check.summary = 'Triggered cluster update for %s - updating all services.' % cgap_candidate[0]
    else:
        if cluster_name not in cluster_arns:
            check.status = 'FAIL'
            check.summary = 'Given cluster name does not exist! Gave: %s, Resolved: %s' % (cluster_name, cluster_arns)
        else:
            client.update_all_services(cluster_name=cluster_name)
            check.status = 'PASS'
            check.summary = 'Triggered cluster update for %s - updating all services.' % cluster_name
    return check


@check_function(github_repo_url='https://github.com/dbmi-bgm/cgap-portal.git',
                github_repo_branch='c4_519', ecr_repo_url=None, env='cgap-mastertest', tag='latest')
def trigger_docker_build(connection, **kwargs):
    """ Triggers a docker build on Lambda, uploading the result to ECR under the given
        repository and tag. ecr_repo_url takes priority over env if both are passed.
    """
    github_repo_url = kwargs.get('github_repo_url')
    github_repo_branch = kwargs.get('github_repo_branch')
    ecr_repo_url = kwargs.get('ecr_repo_url')
    env = kwargs.get('env')
    tag = kwargs.get('tag')
    if ecr_repo_url:
        url = ecr_repo_url
    elif env:
        url = get_ecr_repo_url(env)
    else:
        raise Exception('Did not pass correct arguments to the check. You need to specify'
                        ' either "ecr_repo_url" or an "env" to resolve from.')
    if not url:
        raise Exception('Could not resolve repo URL for env %s: %s' % (env, url))
    check = CheckResult(connection, 'trigger_docker_build')
    full_output = {}
    repo_location = clone_repo_to_temporary_dir(github_repo_url,
                                                name='cgap-portal', branch=github_repo_branch)
    docker_client = DockerUtils()
    ecr_client = ECRUtils()
    auth_info = ecr_client.authorize_user()
    ecr_pass = ecr_client.extract_ecr_password_from_authorization(authorization=auth_info)
    docker_client.login(ecr_repo_uri=auth_info['proxyEndpoint'],
                        ecr_user='AWS',
                        ecr_pass=ecr_pass)

    image, build_log = docker_client.build_image(path=repo_location, tag=tag, rm=True)
    full_output['build_log'] = build_log
    docker_client.tag_image(image=image, tag=tag, ecr_repo_name=url)
    docker_client.push_image(tag=tag, ecr_repo_name=url, auth_config={
        'username': 'AWS',
        'password': ecr_pass
    })
    check.status = 'PASS'
    check.summary = 'Successfully built/tagged/pushed image to ECR.\n' \
                    'Repo: %s, Tag: %s' % (url, tag)
    return check
