import json
from database_query_executor import DatabaseQueryExecutor

query_executor = DatabaseQueryExecutor()

# Функция извлечения информации о комнатах из JSON-файла
# Преобразует содержимое файла в список словарей, из которых производится вставка в БД
def load_rooms_info(rooms_file_path: str):
    with open(rooms_file_path, 'r', encoding='utf-8') as file:
        rooms_info = json.load(file)
    for room in rooms_info:
        query_executor.update_query_execution(
            "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (room['id'], room['name'])
        )
# Функция извлечения информации об учениках из JSON-файла
# Работает способом, аналогичным оному в load_rooms_info
def load_students_info(students_file_path: str):
    with open(students_file_path, 'r', encoding='utf-8') as file:
        students_info = json.load(file)
    for student in students_info:
        query_executor.update_query_execution(
            """
            INSERT INTO students (id, name, birthday, sex, room)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                student['id'],
                student['name'],
                student['birthday'],
                student['sex'],
                student['room']
            )
        )