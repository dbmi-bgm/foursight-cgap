import boto3
from chalicelib.vars import FOURSIGHT_PREFIX

class Queues(object):
    """create and configure queues for foursight"""
    prefix = FOURSIGHT_PREFIX
    suffices = ['dev-check-queue', 'prod-check-queue', 'test-check-queue']

    def __init__(self):
        pass

    @property
    def queue_names(self):
        return [self.prefix + '-' + suffix for suffix in self.suffices]

    def create_queues(self):
        sqs = boto3.client('sqs')
        for queue in self.queue_names:
            sqs.create_queue(QueueName=queue)


def main():
    queues = Queues()
    queues.create_queues()


if __name__ == '__main__':
    main()
