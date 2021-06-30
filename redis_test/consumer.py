import asyncio
import json
from contextlib import suppress

import aioredis
from aioredis.errors import BusyGroupError
from loguru import logger

from redis_test.defect import PredictionMessage


async def reader(channel):
    async for message in channel.iter():
        print('Got message: ', message)


async def main():
    redis = await aioredis.create_redis_pool('redis://localhost', encoding='utf-8')
    with suppress(BusyGroupError):
        await redis.xgroup_create('predictions', 'db-worker', latest_id='0')

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
            _, message_id, message = item
            payload = message['payload']
            prediction = PredictionMessage(**json.loads(payload))
            logger.info(prediction)

asyncio.run(main())