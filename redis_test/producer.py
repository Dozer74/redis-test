import redis

client = redis.Redis(host='localhost', port=6379, db=0)
client.publish('my-queue', 'test')
