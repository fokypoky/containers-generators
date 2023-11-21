import redis
def delete() -> None:
    r = redis.Redis(host='localhost', port=13700, db=0)
    r.flushall()