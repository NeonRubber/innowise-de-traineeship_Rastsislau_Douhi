-- @insert_room
INSERT INTO rooms (id, name) 
VALUES %s 
ON CONFLICT (id) DO NOTHING;

-- @insert_student
INSERT INTO students (id, name, birthday, sex, room)
VALUES %s
ON CONFLICT (id) DO NOTHING;