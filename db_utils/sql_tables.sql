CREATE SCHEMA IF NOT EXISTS orient_data;

CREATE TABLE IF NOT EXISTS orient_data.events (
    event_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    event_date DATE NOT NULL,
    event_name VARCHAR(100),
    city VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS orient_data.group_params (
    group_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    group_name VARCHAR(20),
    cp SMALLINT,
    length_km NUMERIC(6,2),
    event_id INTEGER REFERENCES events(event_id)
);


CREATE TABLE IF NOT EXISTS orient_data.participants (
    participant_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    full_name TEXT NOT NULL,
    birth_year SMALLINT,
    team text[]
);

CREATE TABLE IF NOT EXISTS orient_data.results (
    result_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    event_id INTEGER NOT NULL REFERENCES events(event_id),
    group_id INTEGER NOT NULL REFERENCES group_params(group_id),
    participant_id INTEGER NOT NULL REFERENCES participants(participant_id),
    position_number INTEGER,
    qualification VARCHAR(10),
    bib_number  SMALLINT,
    finish_position  SMALLINT,
    result_time      VARCHAR(20),
    time_gap         VARCHAR(20),
    UNIQUE (event_id, group_id, participant_id)
);


CREATE TABLE IF NOT EXISTS orient_data.pages_processing_log (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    page_name TEXT,
    date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS orient_data.etl_log (
    id SERIAL PRIMARY KEY,
    operation_name TEXT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_sec INT
);