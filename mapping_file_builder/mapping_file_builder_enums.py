from enum import Enum, auto


class PersistenceEnum(Enum):
    pickle = auto()
    msgpack = auto()
    vanilla = auto()
