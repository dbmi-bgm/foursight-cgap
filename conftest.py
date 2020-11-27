import chalice
import unittest
import datetime
import json
import os
import sys
import time
import boto3
import app
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
)
from chalicelib import (
    app_utils,
    decorators,
)
from .vars import *
from dcicutils import s3_utils, ff_utils
from contextlib import contextmanager
import pytest

check_function = decorators.Decorators().check_function
action_function = decorators.Decorators().action_function
DEV_ENV = 'cgapdev'

@pytest.fixture(scope='session', autouse=True)
def setup():
    app.set_stage('test')  # set the stage info for tests
    test_client = boto3.client('sqs')  # purge test queue
    queue_url = sqs_utils.SQS(FOURSIGHT_PREFIX).get_sqs_queue().url
    try:
        test_client.purge_queue(
            QueueUrl=queue_url
        )
    except test_client.exceptions.PurgeQueueInProgress:
        print('Cannot purge test queue; purge already in progress')
