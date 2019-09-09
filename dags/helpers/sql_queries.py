class SqlQueries:
    co2_ppm_insert = ("""SELECT "date",
        "interpolated",
        "trend"
    FROM co2_ppm_staging;
    """)

    glacier_insert = ("""SELECT DISTINCT year,
                    mean_cumulative_mass_balance
    FROM glacier_staging;
    """)

    temperature_insert = ("""SELECT "date",
           MAX(CASE WHEN source = 'GCAG' THEN mean END)    AS "gcag",
           MAX(CASE WHEN source = 'GISTEMP' THEN mean END) AS "gistemp"
    FROM temperature_staging
    GROUP BY "date";
    """)

    population_insert = ("""SELECT "country_code",
           "year",
           "value"
    FROM population_staging;
    """)

    sea_level_insert = ("""
    SELECT "time",
           "gmsl"
    FROM sealevel_staging;
    """)
