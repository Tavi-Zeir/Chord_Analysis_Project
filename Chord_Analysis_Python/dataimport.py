import re
import ast
import csv
import mysql.connector
from typing import Dict
import rootnote as rn
import chordquality
import sqlqueries as sq


class DataImport:
    MYSQL_HOSTNAME = 'localhost'
    MYSQL_USERNAME = 'root'
    DATABASE_NAME = 'chord_analysis'
    CSV_FILEPATH = '../chords_and_lyrics_data/chords_and_lyrics_trimmed.csv'
    VALID_CHORD_SYMBOLS = '#+/'

    connection = None

    def __init__(self, mysql_password):
        self.connection = mysql.connector.connect(
            host=self.MYSQL_HOSTNAME,
            user=self.MYSQL_USERNAME,
            password=mysql_password,
            database=self.DATABASE_NAME)

    def import_data(self,
                    basic_info=False,
                    chord_qualities=False,
                    chord_occurrences=False,
                    start=0,
                    end=float('inf'),
                    filename=CSV_FILEPATH):
        """Reads a csv and records data into the database.
        basic_info: record song title, artist name, and artist genres
        chord_qualities: record all chord qualities that occur
        chord_occurrences: record counts for how many times each category of chord occurs in each song
        Only reads rows from 'start' to 'end'."""
        with open(filename, newline='', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            _ = next(csvreader)  # header row
            for row in csvreader:
                song_id = int(row[0])
                if song_id >= end:
                    break
                if song_id >= start:
                    artist_name = row[1]
                    song_title = row[2]
                    spotify_artist_id = row[3]
                    genres = ast.literal_eval(row[4])  # list of str
                    chord_info = ast.literal_eval(row[5])  # dict int to str

                    if basic_info:
                        self.record_basic_info(song_id, artist_name, song_title, spotify_artist_id, genres)
                    if chord_qualities:
                        self.record_chord_qualities(chord_info)
                    if chord_occurrences:
                        self.record_chord_occurrences(song_id, chord_info)

    def record_basic_info(self, song_id, artist_name, song_title, spotify_artist_id, genres):
        pass

    def record_chord_qualities(self, chord_info):
        """Parses chord info for one song and records found chord quality names
        to the database for future categorization.
        """
        chords = self.parse_chord_info(chord_info)
        qualities_set = set(map(lambda c: c[1], chords))
        cursor = self.connection.cursor(buffered=True)
        for quality in qualities_set:
            cursor.execute(sq.SQLQueries.get_chord_quality_exists, [quality])
            count = cursor.fetchone()
            if count[0] == 0:
                cursor.execute(sq.SQLQueries.add_chord_quality, [quality])
                self.connection.commit()

    def record_chord_occurrences(self, song_id, chord_info: Dict[int, str]):
        """Parses chord info and records the occurrences of each chord
        into the database for one song."""
        chords = self.parse_chord_info(chord_info)

        # see what qualities this song uses and get their category
        qualities_set = set(map(lambda c: c[1], chords))
        categories = {}
        cursor = self.connection.cursor(buffered=True)
        for quality in qualities_set:
            cursor.execute(sq.SQLQueries.get_chord_category, [quality])
            categories[quality] = cursor.fetchone()[0]

        # count how many times each note/category pair comes up in this song
        occurrences = {}
        for chord in chords:
            # root note id and chord quality category id:
            chord_id_pair = (rn.RootNote.get_note_id(chord[0]), categories[chord[1]])
            if chord_id_pair not in occurrences:
                occurrences[chord_id_pair] = 1
            else:
                occurrences[chord_id_pair] += 1

        # add the occurrence data to the database
        for chord_id_pair in occurrences:
            cursor.execute(sq.SQLQueries.add_chord_occurrence,
                           (chord_id_pair[0],
                            chord_id_pair[1],
                            song_id,
                            occurrences[chord_id_pair]))
            self.connection.commit()

    def parse_chord_info(self, chord_info: Dict[int, str]):
        """parses the chord info from the dataset into a list of root note / chord quality pairs"""
        chords = []
        for line in chord_info:
            words = chord_info[line].split()
            for word in words:
                split_result = self.split_chord_name(word)
                if split_result is not None:
                    chords.append(split_result)

        return chords

    def get_progressions(self):
        pass

    @staticmethod
    def split_chord_name(chord):
        """given a chord name in text form, returns its root note and chord quality"""
        match = re.match(r'C#|C|Db|D#|D|Eb|E|F#|F|Gb|G#|G|Ab|A#|A|Bb|B', chord)
        if match:
            return chord[:match.end()], chord[match.end():match.end()+10]
        return None


if __name__ == '__main__':
    data_import = DataImport('P@ssw0rd939')  # Password here
    data_import.import_data(basic_info=True, chord_occurrences=True, end=3)
    data_import.connection.close()
