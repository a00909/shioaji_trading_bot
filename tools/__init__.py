from tools.redis_manager import RedisManager

redis_manager = None
history_tick_manager = None

def init():
    redis_manager = RedisManager()