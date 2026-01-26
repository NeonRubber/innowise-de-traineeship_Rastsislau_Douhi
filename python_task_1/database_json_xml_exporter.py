import json
import xml.etree.ElementTree as Xmletree
from pathlib import Path

# Создание класса-экспортёра данных запроса в JSON/XML
class DataExporter:
    def __init__(self, data):
        self.data = data

    # Метод-экспортёр в JSON-файл
    def export_into_json(self, filepath: Path):
        def convert(object):
            if isinstance(object, (int, float, str, type(None))):
                return object
            elif isinstance(object, dict):
                return {key: convert(value) for key, value in object.items()}
            elif isinstance(object, list):
                return [convert(value) for value in object]
            else:
                return float(object)

        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(convert(self.data), file, ensure_ascii=False, indent=2)

    # Метод-экспортёр в XML-файл
    def export_into_xml(self, filepath: Path):
        root = Xmletree.Element("results")
        for row in self.data:
            item = Xmletree.SubElement(root, "item")
            for key, value in row.items():
                child = Xmletree.SubElement(item, key)
                child.text = str(value)
            xml_tree = Xmletree.ElementTree(root)
            xml_tree.write(filepath, encoding="utf-8", xml_declaration=True)

    # Метод, выбирающий формат экспортируемого файла исходя из конфигурации .env
    def export(self, format: str, filepath: str):
        file_path = Path(filepath)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if format == 'json':
            self.export_into_json(file_path)
        elif format == 'xml':
            self.export_into_xml(file_path)
        else:
            raise ValueError("This file format is unsupported. Make sure you are using JSON or XML.")