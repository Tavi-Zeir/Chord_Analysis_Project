from enum import Enum


class ChordQuality(Enum):
    INVALID = 0  # i.e. not a chord
    MAJOR = 1  # including dominant
    MINOR = 2
    NEUTRAL = 3  # i.e. sus or power chord
    DIMINISHED = 4
    AUGMENTED = 5


