from app.data_loader import LogLoader
from app.alert_rules import FatalErrorChecker, BundleFatalChecker
import time

loader = LogLoader('data/data.csv', chunk_size=100000)

rules = [
    FatalErrorChecker(),
    BundleFatalChecker()
]

print("Starting log monitoring...")

while True:
    chunk = loader.get_chunk()
    
    if chunk is None:
        print("Monitoring has ended.")
        break

    for rule in rules:
        print(f"Warning! The {type(rule).__name__} has been triggered!")