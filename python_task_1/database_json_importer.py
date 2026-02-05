import json
import logging
from typing import List, Tuple, Any, Iterator
from database_query_executor import DatabaseQueryExecutor
from models import Room, Student
from pydantic import ValidationError

# Batching configuration
BATCH_SIZE = 1000

logger = logging.getLogger(__name__)
query_executor = DatabaseQueryExecutor()

def _process_in_batches(data: Iterator, batch_size: int, query: str, log_entity_name: str):
    batch: List[Tuple[Any, ...]] = []
    total_inserted = 0
    
    for item in data:
        batch.append(item)
        if len(batch) >= batch_size:
            query_executor.bulk_insert(query, batch)
            total_inserted += len(batch)
            batch.clear()
            logger.info(f"Inserted batch of {batch_size} {log_entity_name}...")
        
    if batch:
        query_executor.bulk_insert(query, batch)
        total_inserted += len(batch)
    
    return total_inserted

# Accepting query as an argument
def load_rooms_info(rooms_file_path: str, insert_query: str):
    try:
        logger.info(f"Start loading rooms from {rooms_file_path}")
        with open(rooms_file_path, 'r', encoding='utf-8') as file:
            rooms_data = json.load(file)
        
        def valid_rooms_generator() -> Iterator[Tuple[int, str]]:
            for item in rooms_data:
                try:
                    room = Room(**item)
                    yield (room.id, room.name)
                except ValidationError as e:
                    logger.warning(f"Skipping invalid room data: {item}. Error: {e}")

        total = _process_in_batches(
            data=valid_rooms_generator(),
            batch_size=BATCH_SIZE,
            query=insert_query,
            log_entity_name="rooms"
        )
        logger.info(f"Successfully processed {total} rooms.")

    except Exception as e:
        logger.error(f"Failed to load rooms info: {e}")
        raise

# Accepting query as an argument
def load_students_info(students_file_path: str, insert_query: str):
    try:
        logger.info(f"Start loading students from {students_file_path}")
        with open(students_file_path, 'r', encoding='utf-8') as file:
            students_data = json.load(file)

        def valid_students_generator() -> Iterator[Tuple]:
            for item in students_data:
                try:
                    student = Student(**item)
                    yield (
                        student.id,
                        student.name,
                        student.birthday,
                        student.sex,
                        student.room
                    )
                except ValidationError as e:
                    logger.warning(f"Skipping invalid student data: {item}. Error: {e}")

        total = _process_in_batches(
            data=valid_students_generator(),
            batch_size=BATCH_SIZE,
            query=insert_query,
            log_entity_name="students"
        )
        logger.info(f"Successfully processed {total} students.")

    except Exception as e:
        logger.error(f"Failed to load students info: {e}")
        raise