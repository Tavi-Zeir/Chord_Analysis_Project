import re
import ast
import csv
import mysql.connector
from typing import Dict
import rootnote
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

    def first_pass(self, start=0, end=float('inf'), filename=CSV_FILEPATH):
        """reads a csv and adds found chord qualities to the database to set up for the main analysis.
        Only reads rows from 'start' to 'end'.
        """
        total_quality_count = 0
        added_quality_count = 0
        with open(filename, newline='', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            _ = next(csvreader)  # header row
            for row in csvreader:
                song_id = int(row[0])
                if song_id >= end:
                    break
                if song_id >= start:
                    chord_info = ast.literal_eval(row[5])  # dict int to str
                    chords = self.parse_chord_info(chord_info)
                    qualities_set = set(map(lambda c: c[1], chords))
                    cursor = self.connection.cursor(buffered=True)
                    for quality in qualities_set:
                        total_quality_count += 1
                        cursor.execute(sq.SQLQueries.get_chord_quality_exists, [quality])
                        count = cursor.fetchone()
                        if count[0] == 0:
                            added_quality_count += 1
                            cursor.execute(sq.SQLQueries.add_chord_quality, [quality])
                            self.connection.commit()
        self.connection.close()
        print('Total Qualities: ' + str(total_quality_count))
        print('Qualities Added: ' + str(added_quality_count))

    def second_pass(self, start=0, end=float('inf'), filename=CSV_FILEPATH):
        """reads a csv and imports its data to the database. Only reads rows from 'start' to 'end'."""
        with open(filename, newline='') as csvfile:
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
                    pass

    def parse_chord_info(self, chord_info: Dict[int, str]):
        """parses the chord info from the dataset into a list of chord qualities"""
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
    data_import.first_pass(start=160)
