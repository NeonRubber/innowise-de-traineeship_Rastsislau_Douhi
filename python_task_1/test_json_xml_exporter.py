from database_json_xml_exporter import DataExporter

test_data = [
    {"id": 1, "name": "Peggy", "birthday": "2011-08-22T00:00:00", "sex": "F", "room": 101},
    {"id": 2, "name": "John", "birthday": "2010-05-15T00:00:00", "sex": "M", "room": 102}
]

exporter = DataExporter(test_data)

exporter.export("json", "test_json_output.json")
exporter.export("xml", "test_xml_output.xml")