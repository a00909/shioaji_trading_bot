import functools


class CacheManager:
    def __init__(self):
        # 初始化一个共享的嵌套字典用于缓存
        self.cache = {}

    def cache(self, func):
        @functools.wraps(func)
        def wrapper(*args):
            # 生成唯一的缓存键，包括函数名称和参数
            func_name = func.__name__
            if func_name not in self.cache:
                self.cache[func_name] = {}  # 初始化第二层字典

            # 检查缓存中是否已有结果
            if args in self.cache[func_name]:
                print(f"Cache hit for {func_name} with arguments: {args}")
                return self.cache[func_name][args]

            # 如果没有缓存，则调用原始函数并缓存结果
            print(f"Cache miss for {func_name} with arguments: {args}")
            result = func(*args)
            self.cache[func_name][args] = result
            return result

        return wrapper

    def clear_cache(self, func):
        """清除特定方法的缓存"""

        @functools.wraps(func)
        def wrapper(*args):
            func_name = func.__name__
            if func_name in self.cache and args in self.cache[func_name]:
                del self.cache[func_name][args]
                print(f"Cache cleared for {func_name} with arguments: {args}")
            else:
                print(f"No cache found for {func_name} with arguments: {args}")
            return func(*args)

        return wrapper

    def clear_all_caches(self):
        """清除所有方法的缓存"""
        self.cache.clear()
        print("All caches cleared.")
