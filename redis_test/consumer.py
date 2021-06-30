import asyncio
import json
from contextlib import suppress
from typing import Tuple, OrderedDict

import aioredis
from aioredis import Redis
from aioredis.errors import BusyGroupError
from loguru import logger

from redis_test.defect import PredictionMessage


async def db_worker_loop(redis: Redis) -> None:
    while True:
        items = await redis.xread_group(
            'db-worker',
            'db-worker-1',
            ['predictions'],
            count=100,
            latest_ids=['>']
        )
        logger.info('db-worker-1: Read {} messages from "predictions"', len(items))

        for item in items:
            prediction = _parse_payload(item)
            logger.info('db-worker: {}', prediction)


async def fe_worker_loop(redis: Redis) -> None:
    while True:
        item = await redis.xread_group(
            'fe-worker',
            'fe-worker-1',
            ['predictions'],
            count=1,
            latest_ids=['>']
        )
        item = item[0]

        prediction = _parse_payload(item)
        logger.info('fe-worker: {}', prediction)


def _parse_payload(item: Tuple[str, str, OrderedDict]) -> PredictionMessage:
    _, message_id, message = item
    payload = message['payload']
    prediction = PredictionMessage(**json.loads(payload))
    return prediction


async def main():
    redis = await aioredis.create_redis_pool('redis://localhost', encoding='utf-8')
    # TODO: add the second consumer group that will emulate frontend worker.
    #  It should start with the latest message available (from '$', not from '0')
    #  and print it on the screen.

    with suppress(BusyGroupError):
        await redis.xgroup_create('predictions', 'db-worker', latest_id='0')

    with suppress(BusyGroupError):
        await redis.xgroup_create('predictions', 'fe-worker', latest_id='$')

    db_task = asyncio.create_task(db_worker_loop(redis))
    fe_task = asyncio.create_task(fe_worker_loop(redis))
    await asyncio.gather(db_task, fe_task)


asyncio.run(main())
