-- Indexes to optimize JOIN and GROUP BY operations
CREATE INDEX IF NOT EXISTS idx_students_room ON students(room);
CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);
CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);
