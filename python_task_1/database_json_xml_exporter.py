import json
import xml.etree.ElementTree as Xmletree
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class DataExporter:
    # Exports query results to JSON or XML
    
    def __init__(self, data):
        # Expects list of dicts
        self.data = data

    def _json_serializer(self, obj):
        # Serializes dates and decimals for JSON
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        # Fallback for other types
        return str(obj)

    def export_into_json(self, filepath: Path):
        """Exports data to a JSON file."""
        try:
            with open(filepath, 'w', encoding='utf-8') as file:
                json.dump(
                    self.data, 
                    file, 
                    default=self._json_serializer, 
                    ensure_ascii=False, 
                    indent=2
                )
            logger.info(f"JSON exported to {filepath}")
        except Exception as e:
            logger.error(f"Failed to export JSON: {e}")
            raise

    def export_into_xml(self, filepath: Path):
        # Exports data to an XML file.
        try:
            root = Xmletree.Element("results")
            
            for row in self.data:
                item = Xmletree.SubElement(root, "item")
                for key, value in row.items():
                    child = Xmletree.SubElement(item, key)
                    # Handle NULL values
                    if value is None:
                        child.text = "" 
                    else:
                        child.text = str(value)
            
            xml_tree = Xmletree.ElementTree(root)
            # Use built-in indentation if available
            if hasattr(Xmletree, 'indent'):
                Xmletree.indent(xml_tree, space="  ", level=0)
            
            xml_tree.write(filepath, encoding="utf-8", xml_declaration=True)
            logger.info(f"XML exported to {filepath}")
        except Exception as e:
            logger.error(f"Failed to export XML: {e}")
            raise

    def export(self, format: str, filepath: str):
        # Selects format and exports data
        file_path = Path(filepath)
        # Create directories if needed
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Error creating directory for {file_path}: {e}")
            raise

        if format.lower() == 'json':
            self.export_into_json(file_path)
        elif format.lower() == 'xml':
            self.export_into_xml(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")