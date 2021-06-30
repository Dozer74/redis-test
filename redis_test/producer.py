import random
import time
from datetime import datetime

import click
import redis
from loguru import logger

from redis_test.defect import Defect, PredictionMessage, DEFAULT_DEFECTS


@click.command()
@click.option('--stream', default='predictions')
@click.option('--messages', default=100)
@click.option('--delay', default=1)
def main(stream, messages, delay):
    client = redis.Redis(host='localhost', port=6379, db=0)
    logger.info(
        'Producing {n} messages to {stream} stream. Delay: {delay}',
        n=messages, stream=stream, delay=delay
    )

    weights = [d.weight for d in DEFAULT_DEFECTS]

    for i in range(messages):
        defect = None
        if random.uniform(0, 1) >= 0.33:
            descriptor = random.choices(DEFAULT_DEFECTS, weights, k=1)[0]
            defect = Defect(id=descriptor.id, name=descriptor.name, severity=descriptor.severity)

        payload = PredictionMessage(
            id=i + 1,
            timestamp=datetime.now(),
            defect=defect
        ).json()
        message = {'payload': payload}

        logger.info('Publish new prediction: {}', message)

        client.xadd(stream, message, maxlen=1000)
        time.sleep(delay)


if __name__ == '__main__':
    main()
