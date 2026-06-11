CREATE KEYSPACE IF NOT EXISTS flight_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_data;

CREATE TABLE IF NOT EXISTS origin_dest_distances (
    origin text,
    dest text,
    distance double,
    PRIMARY KEY ((origin, dest))
);

CREATE TABLE IF NOT EXISTS flight_predictions (
    uuid       text PRIMARY KEY,
    prediction text,
    timestamp  timestamp,
    origin     text,
    dest       text,
    carrier    text,
    dep_delay  double
);
