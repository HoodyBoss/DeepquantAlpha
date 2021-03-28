import redis


class CacheProxy():

    def __init__(self, host, port, password=None):
        if password == None:
            self.cache = redis.Redis(host=host, port=port)
        else:
            self.cache = redis.Redis(host=host, port=port, password=password)

    def get_cache_instance(self):
        return self.cache

    def set(self, key, value):
        self.cache.set(key, value)

    def get(self, key):
        return self.cache.get(key)

    def delete(self, *names):
        self.cache.delete(names)
