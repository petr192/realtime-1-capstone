from collections import OrderedDict
from time import monotonic


class TTLCache:
    def __init__(self, ttl=30, max_entries=128):
        if ttl <= 0 or max_entries < 1:
            raise ValueError("TTL and capacity must be positive")
        self.ttl = ttl
        self.max_entries = max_entries
        self.entries = OrderedDict()

    def get(self, key):
        entry = self.entries.get(key)
        if entry is None:
            return None
        response, expires_at = entry
        if monotonic() >= expires_at:
            del self.entries[key]
            return None
        return response

    def put(self, key, response):
        self.entries[key] = (response, monotonic() + self.ttl)
        self.entries.move_to_end(key)
        while len(self.entries) > self.max_entries:
            self.entries.popitem(last=False)

