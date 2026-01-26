from database_json_importer import load_rooms_info, load_students_info
from database_query_executor import DatabaseQueryExecutor
from database_json_xml_exporter import DataExporter
from dotenv import load_dotenv
import os

# Необходимые для исполнения SQL-запросы, сохранённые в форме словаря
DATABASE_QUERIES = {
    "rooms_with_students_counts": """
    SELECT
        rooms.id,
        rooms.name,
        COUNT(students.id) AS _students_count_in_the_room
    FROM rooms
    LEFT JOIN students on rooms.id = students.room
    GROUP BY rooms.id, rooms.name
    ORDER BY rooms.id
    """,

    "five_rooms_with_the_smallest_average_age": """
        SELECT
            rooms.id,
            rooms.name,
            AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) AS average_age
        FROM rooms
        LEFT JOIN students on rooms.id = students.room
        GROUP BY rooms.id, rooms.name
        ORDER BY average_age
        LIMIT 5
    """,

    "five_rooms_with_the_largest_age_difference": """
        SELECT
            rooms.id,
            rooms.name,
            MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) -
            MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) AS age_difference
        FROM rooms
        INNER JOIN students on rooms.id = students.room
        GROUP BY rooms.id, rooms.name
        ORDER BY age_difference
        LIMIT 5
    """,

    "rooms_with_mixed_sex_students": """
        SELECT
            rooms.id,
            rooms.name
        FROM rooms
        INNER JOIN students on rooms.id = students.room
        GROUP BY rooms.id, rooms.name
        HAVING COUNT(DISTINCT students.sex) > 1
    """
}

load_dotenv()

def main():
    students_file_path = os.getenv("STUDENTS_FILE_PATH")
    rooms_file_path = os.getenv("ROOMS_FILE_PATH")
    output_format = os.getenv("EXPORT_DATA_FORMAT")
    output_path = os.getenv("EXPORT_PATH")

    # Загрузка данных о комнатах и о студентах (именно в таком порядке из-за отношения один-ко-многим)
    print("Getting room info")
    load_rooms_info(rooms_file_path)
    print("Getting students info")
    load_students_info(students_file_path)

    query_executor = DatabaseQueryExecutor()

    # Цикл, по очереди выполняющий заложенные в словарь DATABASE_QUERIES SQL-запросы
    for sql_query_name, sql_query_contents in DATABASE_QUERIES.items():
        print(f"Executing '{sql_query_name}' query")
        query_result = query_executor.select_query_execution(sql_query_contents)
        data_exporter = DataExporter(query_result)
        output_file = f"{output_path}{sql_query_name}.{output_format}"
        data_exporter.export(output_format, output_file)
        print(f"Results are exported into {output_file}")

main()