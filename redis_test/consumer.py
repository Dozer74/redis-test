import asyncio

import aioredis


async def reader(channel):
    async for message in channel.iter():
        print('Got message: ', message)


async def main():
    redis = await aioredis.create_redis_pool('redis://localhost')
    channel = await redis.subscribe('my-queue')
    channel = channel[0]

    loop = asyncio.get_running_loop()
    loop.create_task(reader(channel))

    try:
        await asyncio.sleep(1000)
    except KeyboardInterrupt:
        pass

    redis.close()
    await redis.wait_closed()


asyncio.run(main())
