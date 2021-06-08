import time

import redis

client = redis.Redis(host='localhost', port=6379, db=0)
for i in range(100):
    client.publish('my-queue', f'prediction {i + 1}')
    time.sleep(1)
