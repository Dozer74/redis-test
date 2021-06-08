import redis

client = redis.Redis(host='localhost', port=6379, db=0)
ps = client.pubsub()
ps.subscribe('my-queue')

for item in ps.listen():
    if item['type'] == 'message':
        print(item)
