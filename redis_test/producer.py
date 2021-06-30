import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

import click
import redis
from loguru import logger
from pydantic import BaseModel


class Defect(BaseModel):
    id: int
    name: str
    severity: int


class PredictionMessage(BaseModel):
    id: int
    timestamp: datetime
    defect: Optional[Defect]


@dataclass
class FakeDefectDescriptor:
    """ Describes a fake defect metadata
    """
    id: int
    name: str
    weight: float
    severity: int


DEFAULT_DEFECTS = [
    FakeDefectDescriptor(1, 'LFC', 0.3, 5),
    FakeDefectDescriptor(2, 'Breakout', 0.1, 10),
    FakeDefectDescriptor(3, 'Sliver', 0.2, 7),
    FakeDefectDescriptor(4, 'Fake Defect', 0.4, 2)
]


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
