"""
Generate gitignored .chalice/config.json for deploy and then run deploy.
Takes on parameter for now: stage (either "dev" or "prod")
"""
from os.path import dirname
import argparse
from foursight_core.package import PackageDeploy as PackageDeploy_from_core


class PackageDeploy(PackageDeploy_from_core):

    CONFIG_BASE = PackageDeploy_from_core.CONFIG_BASE
    CONFIG_BASE['app_name'] = 'foursight-cgap'

    config_dir = dirname(__file__)


def main():
    parser = argparse.ArgumentParser('chalice_package')
    parser.add_argument(
        'stage',
        type=str,
        choices=['dev', 'prod'],
        help="chalice package stage. Must be one of 'prod' or 'dev'")
    parser.add_argument(
        'output_file',
        type=str,
        help='Directory where generated template should be written')
    parser.add_argument(
        '--merge_template',
        type=str,
        help='Location of a YAML template to be merged into the generated template')
    args = parser.parse_args()
    PackageDeploy.build_config_and_package(args)


if __name__ == '__main__':
    main()
