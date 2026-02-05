import os
import logging

logger = logging.getLogger(__name__)

# Loads SQL queries from an external file into a dictionary

def load_sql_from_file(file_path: str) -> dict:
    queries = {}
    if not os.path.exists(file_path):
        logger.warning(f"SQL file {file_path} not found.")
        return queries
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        current_name = None
        current_query = []
        
        for line in content.splitlines():
            if line.strip().startswith("-- @"):
                if current_name:
                    queries[current_name] = "\n".join(current_query).strip()
                current_name = line.strip().replace("-- @", "").strip()
                current_query = []
            else:
                current_query.append(line)
                
        if current_name and current_query:
            queries[current_name] = "\n".join(current_query).strip()
            
        return queries
    except Exception as e:
        logger.error(f"Error parsing SQL file {file_path}: {e}")
        return {}