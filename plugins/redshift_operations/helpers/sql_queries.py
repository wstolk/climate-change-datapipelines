class SqlQueries:
    songplay_table_insert = ("""
    SELECT
        DISTINCT timestamp 'epoch' + (ts / 1000) * interval '1 second' AS start_time,
        userid AS user_id,
        level,
        song_id,
        artist_id,
        sessionid AS session_id,
        location,
        useragent AS user_agent
    FROM staging_events
    LEFT JOIN
        staging_songs
        ON (staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name)
    WHERE
        page = 'NextSong'
        AND song_id IS NOT NULL
        AND artist_id IS NOT NULL;
    """)

    user_table_insert = ("""
    SELECT
        DISTINCT userid AS user_id,
        firstname AS first_name,
        lastname AS last_name,
        gender,
        level
    FROM staging_events
    WHERE 
        user_id IS NOT NULL
        AND page = 'NextSong';
    """)

    song_table_insert = ("""
    SELECT 
        DISTINCT song_id,
        title AS name,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
    FROM staging_songs
    WHERE 
        song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
    SELECT
        DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS lattitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE 
        artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
    SELECT
        DISTINCT start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(DOW FROM start_time) AS weekday
    FROM songplays;
    """)
