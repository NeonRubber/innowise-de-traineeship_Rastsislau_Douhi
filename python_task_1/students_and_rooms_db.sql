CREATE TABLE rooms (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE students (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    birthday TIMESTAMP NOT NULL,
    sex CHAR(1) NOT NULL,
    room INTEGER REFERENCES rooms(id)
);