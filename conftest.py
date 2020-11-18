from __future__ import print_function, unicode_literals
import chalice
import unittest
import datetime
import json
import os
import sys
import time
import boto3
import app
from foursight_core.chalicelib import (
    sys_utils,
    fs_connection,
    s3_connection,
    es_connection,
    exceptions
)
from foursight_core.chalicelib.utils import *
from chalicelib import (
    sqs_utils,
    app_utils,
    check_utils,
    decorators,
    run_result,
    config,
)
from chalicelib.vars import *
from dcicutils import s3_utils, ff_utils
from dateutil import tz
from contextlib import contextmanager
import pytest

check_function = decorators.Decorators.check_function
action_function = decorators.Decorators.action_function

@pytest.fixture(scope='session', autouse=True)
def setup():
    app.set_stage('test')  # set the stage info for tests
    test_client = boto3.client('sqs')  # purge test queue
    queue_url = sqs_utils.SQS.get_sqs_queue().url
    try:
        test_client.purge_queue(
            QueueUrl=queue_url
        )
    except test_client.exceptions.PurgeQueueInProgress:
        print('Cannot purge test queue; purge already in progress')
