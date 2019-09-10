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

    country_insert = ("""
    SELECT "country_code",
           "shortname",
           "alpha_code",
           "currency_unit",
           "region",
           "income_group"
    FROM countries_staging;
    """)

    indicator_insert = ("""
    SELECT "indicator_code",
        "country_code",
        "year",
        "value"
    FROM indicators_staging;
    """)