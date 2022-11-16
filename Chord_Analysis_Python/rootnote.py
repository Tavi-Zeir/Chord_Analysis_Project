from enum import Enum


class RootNote(Enum):
    C = (0, None)
    Db = (1, 'C#')
    D = (2, None)
    Eb = (3, 'D#')
    E = (4, None)
    F = (5, None)
    Gb = (6, 'F#')
    G = (7, None)
    Ab = (8, 'G#')
    A = (9, None)
    Bb = (10, 'A#')
    B = (11, None)

    def __init__(self, note_id, other_name):
        self.note_id = note_id
        self.other_name = other_name

    # given a string with a note name, returns the root_note_id
    @classmethod
    def get_note_id(cls, note_name):
        for note in cls:
            if note_name == note.name or note.other_name:
                return note.note_id
