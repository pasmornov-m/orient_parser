import psycopg2


def create_database():
    conn = psycopg2.connect(
        host='db',
        database='postgres',
        user='postgres',
        password='postgres',
        port=5432
    )

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'orient_data'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE orient_data")

    cursor.close()
    conn.autocommit = False

def create_tables():
    conn_orient = psycopg2.connect(
        host='db',
        database='orient_data',
        user='postgres',
        password='postgres',
        port=5432
    )

    cursor = conn_orient.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS events(
            event_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            event_date DATE NOT NULL,
            event_name VARCHAR(100),
            city VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS group_params(
            group_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            group_name VARCHAR(20),
            cp SMALLINT,
            length_km NUMERIC(6,2),
            event_id INTEGER REFERENCES events(event_id)
        );

        CREATE TABLE IF NOT EXISTS participants(
            participant_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            full_name TEXT NOT NULL,
            birth_year SMALLINT,
            team text[]
        );

        CREATE TABLE IF NOT EXISTS results(
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

        CREATE TABLE IF NOT EXISTS pages_processing_log(
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            page_name TEXT,
            date DATE DEFAULT CURRENT_DATE
        );
        """)

    cursor.close()
    conn_orient.commit()
    conn_orient.close()
