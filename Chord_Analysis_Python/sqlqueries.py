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