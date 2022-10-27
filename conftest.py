import chalice
import unittest
import datetime
import json
import os
import sys
import time
import boto3
from chalicelib_cgap.app import set_stage
from foursight_core import (
    fs_connection,
    s3_connection,
    es_connection,
    exceptions,
    check_schema,
    stage,
    environment,
    sqs_utils,
    run_result,
    check_utils,
    decorators,
)
from chalicelib_cgap import (
    app_utils,
)
from chalicelib_cgap.vars import *
from chalicelib_cgap import __file__ as chalicelib_path
from chalicelib_cgap.checks.helpers.confchecks import *
from dcicutils import s3_utils, ff_utils
from contextlib import contextmanager
import pytest

# This file basically just exports all of the above imports
# so they are available by the import name above in all the
# test files. Probably not a great method, but remains for historical reasons -Will Oct 7 2021


@pytest.fixture(scope='session', autouse=True)
def setup():
    """ Purge the queues on the "test" stage (should be empty anyway), but
        note that this strategy might cause test runs to interfere with one another.
    """
    set_stage('test')  # set the stage info for tests
    test_client = boto3.client('sqs')  # purge test queue
    queue_url = sqs_utils.SQS(FOURSIGHT_PREFIX).get_sqs_queue().url
    try:
        test_client.purge_queue(
            QueueUrl=queue_url
        )
    except test_client.exceptions.PurgeQueueInProgress:
        print('Cannot purge test queue; purge already in progress')
