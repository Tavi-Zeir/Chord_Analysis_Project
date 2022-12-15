import rootnote
import chordquality


class Chord:
    def __init__(self, note: rootnote.RootNote, quality: chordquality.ChordQuality):
        self.note = note
        self.quality = quality

    def __eq__(self, other):
        if type(self) == type(other):
            return self.note == other.note and self.quality == other.quality
        return False
