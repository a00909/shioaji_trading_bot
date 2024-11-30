import os

from redis.client import Redis


class RedisManager:
    """
    for ticks data
    """
    def __init__(self):
        self.redis = Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=os.getenv('REDIS_PORT', 6379)
        )

    def get(self, key):
        return self.redis.get(key)

    def set(self, key, data: list):
        self.redis.lpush(key, *data)


