import threading
from collections import OrderedDict


class LRUCache:
    """Fixed-size LRU. One lock guards the dict; every operation is O(1).
    Capacity 0 turns the cache off."""

    def __init__(self, capacity: int):
        if capacity < 0:
            raise ValueError("capacity must be >= 0")
        self.capacity = capacity
        self._data: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        """Return (hit, value); a hit makes the key the most recently used."""
        with self._lock:
            if key not in self._data:
                return False, None
            self._data.move_to_end(key)
            return True, self._data[key]

    def put(self, key, value) -> None:
        if self.capacity == 0:
            return
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            if len(self._data) > self.capacity:
                self._data.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def keys(self) -> list:
        with self._lock:
            return list(self._data)
