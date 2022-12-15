import re
import ast
import csv
import mysql.connector
import rootnote as rn
import chordquality as cq
import chord as ch
import sqlqueries as sq


class DataImport:
    MYSQL_HOSTNAME = 'localhost'
    MYSQL_USERNAME = 'root'
    DATABASE_NAME = 'chord_analysis'
    CSV_FILEPATH = '../chords_and_lyrics_data/chords_and_lyrics_trimmed.csv'
    MIN_PROGRESSION_LENGTH = 2
    MAX_PROGRESSION_LENGTH = 8

    connection = None

    def __init__(self, mysql_password):
        self.connection = mysql.connector.connect(
            host=self.MYSQL_HOSTNAME,
            user=self.MYSQL_USERNAME,
            password=mysql_password,
            database=self.DATABASE_NAME)
        self.cursor = self.connection.cursor(buffered=True)

    def import_data(self,
                    basic_info=False,
                    chord_qualities=False,
                    chord_occurrences=False,
                    chord_progressions=False,
                    start=0,
                    end=float('inf'),
                    filename=CSV_FILEPATH):
        """Reads a csv and records data into the database.
        basic_info: record song title, artist name, and artist genres
        chord_qualities: record all chord qualities that occur
        chord_occurrences: record counts for how many times each category of chord occurs in each song
        chord_progressions: record what progressions occur in each song
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

                    if chord_qualities or chord_occurrences or chord_progressions:
                        # parse chord info
                        chords = self.parse_chord_info(chord_info)

                    if basic_info:
                        self.record_basic_info(song_id, artist_name, song_title, spotify_artist_id, genres)
                    if chord_qualities:
                        self.record_chord_qualities(chords)
                    if chord_occurrences:
                        self.record_chord_occurrences(song_id, chords)
                    if chord_progressions:
                        self.record_chord_progressions(song_id, chords)

    def record_basic_info(self, song_id, artist_name, song_title, spotify_artist_id, genres):
        """Records basic artist, genre, and song info to the database for one song."""
        # add/update artist and genre info
        self.cursor.execute(sq.SQLQueries.get_artist_name, (spotify_artist_id,))
        stored_name = self.cursor.fetchone()
        if stored_name is None:
            self.cursor.execute(sq.SQLQueries.add_artist, (spotify_artist_id, artist_name))

            # add genre info for new artist
            self.add_genre_info(spotify_artist_id, genres)

        elif stored_name[0] != artist_name:
            # This shouldn't happen in practice. Update to new artist, but notify user.
            print(f'Updating name of artist id {spotify_artist_id} from {stored_name[0]} to {artist_name}')
            self.cursor.execute(sq.SQLQueries.update_artist, (artist_name, spotify_artist_id))

            # clear genre mappings and add new ones
            self.cursor.execute(sq.SQLQueries.delete_genre_mappings_of_artist, (spotify_artist_id,))
            self.add_genre_info(spotify_artist_id, genres)

        # add song info
        self.cursor.execute(sq.SQLQueries.get_song, (song_id,))
        stored_song = self.cursor.fetchone()
        if stored_song is None:
            self.cursor.execute(sq.SQLQueries.add_song, (song_id, song_title, spotify_artist_id))
            self.connection.commit()
        elif stored_song[0] != song_title or stored_song[1] != spotify_artist_id:
            # This shouldn't happen in practice. Update to new song, but notify user.
            print(f'Updating song id {song_id} from title {stored_song[0]} to {song_title}'
                  f' and from artist id {stored_song[1]} to {spotify_artist_id}')
            self.cursor.execute(sq.SQLQueries.update_song, (song_title, spotify_artist_id, song_id))
            self.connection.commit()

    def record_chord_qualities(self, chords):
        """Records chord quality names to the database for future categorization for one song.
        """
        qualities_set = set(map(lambda c: c[1], chords))
        for quality in qualities_set:
            self.cursor.execute(sq.SQLQueries.get_chord_quality_exists, (quality,))
            count = self.cursor.fetchone()
            if count[0] == 0:
                self.cursor.execute(sq.SQLQueries.add_chord_quality, (quality,))
                self.connection.commit()

    def record_chord_occurrences(self, song_id, chords):
        """Records the occurrences of each chord into the database for one song."""
        # see what qualities this song uses and get their category
        qualities_set = set(map(lambda c: c[1], chords))
        categories = {}
        for quality in qualities_set:
            self.cursor.execute(sq.SQLQueries.get_chord_category, (quality,))
            categories[quality] = self.cursor.fetchone()[0]

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
            self.cursor.execute(sq.SQLQueries.add_chord_occurrence,
                                (chord_id_pair[0],
                                 chord_id_pair[1],
                                 song_id,
                                 occurrences[chord_id_pair]))
            self.connection.commit()

    def record_chord_progressions(self, song_id, chords):
        # see what qualities this song uses and get their category
        qualities_set = set(map(lambda c: c[1], chords))
        categories = {}
        for quality in qualities_set:
            self.cursor.execute(sq.SQLQueries.get_chord_category, (quality,))
            categories[quality] = self.cursor.fetchone()[0]

        # get chords as chord objects
        chord_objects = []
        for chord in chords:
            # root note id and chord quality category id:
            chord_objects.append(ch.Chord(rn.RootNote.get_note_id(chord[0]),
                                          cq.ChordQuality(categories[chord[1]])))

        progressions = self.get_progressions(chord_objects)
        for progression in progressions:
            n = len(progression)
            for i in range(n):
                pass
                # TODO check if progression already exists in database, add it if not, and add pairing to song

    def parse_chord_info(self, chord_info: dict[int, str]):
        """parses the chord info from the dataset into a list of root note / chord quality pairs"""
        chords = []
        for line in chord_info:
            words = chord_info[line].split()
            for word in words:
                split_result = self.split_chord_name(word)
                if split_result is not None:
                    chords.append(split_result)

        return chords

    def add_genre_info(self, artist_id, genres):
        """Adds genre info for an artist. Assumes this artist does not already have genres assigned"""
        for genre in genres:
            self.cursor.execute(sq.SQLQueries.get_genre_id_by_name, (genre,))
            genre_id = self.cursor.fetchone()
            if genre_id is None:
                # first time we've seen this genre, add it to the table
                self.cursor.execute(sq.SQLQueries.add_genre, (genre,))
                self.connection.commit()

                # now, get the genre's id
                self.cursor.execute(sq.SQLQueries.get_genre_id_by_name, (genre,))
                genre_id = self.cursor.fetchone()

            # finally, add the artist-genre mapping to the mappings table
            self.cursor.execute(sq.SQLQueries.add_artist_genre_mapping, (artist_id, genre_id[0]))
            self.connection.commit()

    def get_progressions(self, chords: list[ch.Chord]) -> list[list[ch.Chord]]:
        """Given a list of chords, returns a list of the progressions in those chords.
        Defines a progression as a sequence of chords that plays at least twice in a row"""
        progressions = []
        n = len(chords)
        for p_length in range(self.MIN_PROGRESSION_LENGTH, self.MAX_PROGRESSION_LENGTH):
            i = 0
            while i < (n + 1 - 2 * p_length):
                is_valid_progression = True
                for j in range(p_length):
                    if chords[i+j] != chords[i+j+p_length]:
                        is_valid_progression = False
                        break
                if is_valid_progression:
                    progression = chords[i:i+p_length]
                    if progression not in progressions:
                        progressions.append(progression)
                        i += p_length * 2
                    else:
                        is_valid_progression = False
                if not is_valid_progression:
                    i += 1
        return progressions

    @staticmethod
    def split_chord_name(chord):
        """given a chord name in text form, returns its root note and chord quality"""
        match = re.match(r'C#|C|Db|D#|D|Eb|E|F#|F|Gb|G#|G|Ab|A#|A|Bb|B', chord)
        if match:
            return chord[:match.end()], chord[match.end():match.end()+10]
        return None


if __name__ == '__main__':
    password = input('Please enter mySQL password')
    data_import = DataImport(password)
    data_import.import_data(basic_info=True, chord_occurrences=True)
    data_import.connection.close()
