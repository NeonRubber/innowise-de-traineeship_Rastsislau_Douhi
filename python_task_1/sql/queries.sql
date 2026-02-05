-- @rooms_with_students_counts
SELECT
    rooms.id,
    rooms.name,
    COUNT(students.id) AS students_count
FROM rooms
LEFT JOIN students on rooms.id = students.room
GROUP BY rooms.id, rooms.name
ORDER BY rooms.id;

-- @five_rooms_with_the_smallest_average_age
SELECT
    rooms.id,
    rooms.name,
    AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) AS average_age
FROM rooms
LEFT JOIN students on rooms.id = students.room
GROUP BY rooms.id, rooms.name
ORDER BY average_age
LIMIT 5;

-- @five_rooms_with_the_largest_age_difference
SELECT
    rooms.id,
    rooms.name,
    MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) -
    MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) AS age_difference
FROM rooms
INNER JOIN students on rooms.id = students.room
GROUP BY rooms.id, rooms.name
ORDER BY age_difference DESC
LIMIT 5;

-- @rooms_with_mixed_sex_students
SELECT
    rooms.id,
    rooms.name
FROM rooms
INNER JOIN students on rooms.id = students.room
GROUP BY rooms.id, rooms.name
HAVING COUNT(DISTINCT students.sex) > 1;
