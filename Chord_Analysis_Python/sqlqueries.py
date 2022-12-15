import chord as ch


class SQLQueries:
    # chord_qualities:
    get_chord_quality_exists = (
        "SELECT COUNT(*) FROM chord_qualities "
        "WHERE chord_quality = %s"
    )

    add_chord_quality = (
        "INSERT INTO chord_qualities (chord_quality) "
        "VALUES (%s)"
    )

    get_chord_category = (
        "SELECT chord_category FROM chord_qualities "
        "WHERE chord_quality = %s"
    )

    add_chord_occurrence = (
        "INSERT INTO chord_occurrences (root_note_id, chord_category, song_id, count) "
        "VALUES (%s, %s, %s, %s)"
    )

    get_artist_name = (
        "SELECT name FROM artists "
        "WHERE artist_id = %s"
    )

    add_artist = (
        "INSERT INTO artists (artist_id, name) "
        "VALUES (%s, %s)"
    )

    update_artist = (
        "UPDATE artists "
        "SET name = %s "
        "WHERE artist_id = %s"
    )

    get_genre_id_by_name = (
        "SELECT genre_id FROM genres "
        "WHERE name = %s"
    )

    add_genre = (
        "INSERT INTO genres (name) "
        "VALUES (%s)"
    )

    add_artist_genre_mapping = (
        "INSERT INTO artist_genre_mappings (artist_id, genre_id) "
        "VALUES (%s, %s)"
    )

    delete_genre_mappings_of_artist = (
        "DELETE FROM artist_genre_mappings "
        "WHERE artist_id = %s"
    )

    get_song = (
        "SELECT title, artist_id FROM songs "
        "WHERE song_id = %s"
    )

    add_song = (
        "INSERT INTO songs (song_id, title, artist_id) "
        "VALUES (%s, %s, %s)"
    )

    update_song = (
        "UPDATE songs "
        "SET title = %s, artist_id = %s "
        "WHERE song_id = %s"
    )

    def get_matching_progression(self, chords: list[ch.Chord]):
        query = (
            "SELECT progressions_progression_id FROM progression_chords "
            "WHERE "  # TODO finish this
        )
