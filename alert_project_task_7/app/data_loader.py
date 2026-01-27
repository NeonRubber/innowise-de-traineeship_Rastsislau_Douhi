import pandas as pd

class LogLoader:
    def __init__(self, file_path, chunk_size=10000):
        self.file_path = file_path
        self.chunk_size = chunk_size

        self.column_names = [
            'error_code', 'error_message', 'severity', 'log_location', 'mode', 
            'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 
            'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 
            'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date'
        ]

        self.reader = pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size,
            names=self.column_names,
            header=None
        )
    
    def get_chunk(self):
        try:
            return next(self.reader, None)
        except StopIteration:
            return None
        except Exception:
           print("File processing has been stopped due to exception.")
           return None