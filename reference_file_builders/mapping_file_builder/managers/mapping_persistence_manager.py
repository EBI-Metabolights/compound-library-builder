import datetime
import json
import pickle
from typing import Union, Tuple, Any

import msgpack

from compound_common.timer import Timer


class MappingPersistenceManager:
    def __init__(self, root: str, timers_enabled: bool):
        self.root = root
        self.timers_enabled = timers_enabled
        self.pickle = PickleClient(self.root, self.timers_enabled)
        self.msgpack = MessagePackClient(self.root, self.timers_enabled)
        self.vanilla = VanillaJsonClient(self.root, self.timers_enabled)


class PickleClient:
    def __init__(self, root, timers_enabled: bool):
        self.root = root
        self.timers_enabled = timers_enabled

    def save(self, obj, filename) -> Union[None, Timer]:
        timer = Timer(datetime.datetime.now()) if self.timers_enabled else None
        with open(f"{self.root}/{filename}.pickle", "wb") as f:
            pickle.dump(obj, f)
            if timer is not None:
                timer.end = datetime.datetime.now()
        return timer

    def load(self, filename) -> Tuple[Any, Union[None, Timer]]:
        timer = Timer(datetime.datetime.now()) if self.timers_enabled else None
        with open(f"{self.root}/{filename}.pickle", "rb") as f:
            file = pickle.load(f)
            if timer is not None:
                timer.end = datetime.datetime.now()
                return file, timer
            return file


class VanillaJsonClient:
    def __init__(self, root, timers_enabled: bool):
        self.root = root
        self.timers_enabled = timers_enabled

    def save(self, obj, filename) -> Union[None, Timer]:
        timer = Timer(datetime.datetime.now()) if self.timers_enabled else None
        with open(f"{self.root}/{filename}.json", "w") as f:
            json.dump(obj, f)
            if timer is not None:
                timer.end = datetime.datetime.now()
        return timer

    def load(self, filename) -> Tuple[Any, Union[None, Timer]]:
        timer = Timer(datetime.datetime.now()) if self.timers_enabled else None
        with open(f"{self.root}/{filename}.json", "r") as f:
            file = json.load(f)
            if timer is not None:
                timer.end = datetime.datetime.now()
                return file, timer
            return file


class MessagePackClient:
    def __init__(self, root, timers_enabled: bool):
        self.root = root
        self.timers_enabled = timers_enabled

    def save(self, obj, filename) -> Union[None, Timer]:
        timer = Timer(datetime.datetime.now()) if self.timers_enabled else None
        packed = msgpack.packb(obj)
        with open(f"{self.root}/{filename}.bin", "wb") as f:
            f.write(packed)
            if timer is not None:
                timer.end = datetime.datetime.now()
        return timer

    def load(self, filename) -> Tuple[Any, Union[None, Timer]]:
        timer = Timer(datetime.datetime.now()) if self.timers_enabled else None
        with open(f"{self.root}/{filename}.bin", "rb") as f:
            bin = f.read()
            unpacked = msgpack.unpackb(bin)
            if timer is not None:
                timer.end = datetime.datetime.now()
                return unpacked, timer
            return unpacked
