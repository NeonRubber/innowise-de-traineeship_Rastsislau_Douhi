-- init-db/init_schema.sql

CREATE TABLE IF NOT EXISTS rooms (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birthday TIMESTAMP,
    sex VARCHAR(10),
    room INTEGER,
    FOREIGN KEY (room) REFERENCES rooms(id)
);