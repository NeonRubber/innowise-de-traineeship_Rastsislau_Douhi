import pandas as pd
from abc import ABC, abstractmethod

class BaseAlertTrigger(ABC):
    @abstractmethod
    def check(self, chunk: pd.DataFrame) -> bool:
        pass

class FatalErrorChecker(BaseAlertTrigger):
    def check(self, chunk: pd.DataFrame):
        fatal_rows = chunk[chunk['severity'] == 'Error'].copy()

        if len(fatal_rows) == 0:
            return False

        fatal_rows['date'] = pd.to_datetime(fatal_rows['date'], unit='s')
        fatal_rows = fatal_rows.set_index(fatal_rows['date'])

        fatal_counts = fatal_rows.resample('1min').size()

        if (fatal_counts['severity'] > 10).any():
            return True
        
        return False
    
class BundleFatalChecker(BaseAlertTrigger):
    def check(self, chunk: pd.DataFrame):
        fatal_rows = chunk[chunk['severity'] == 'Error'].copy()
        
        if len(fatal_rows) == 0:
            return False
            
        fatal_rows['date'] = pd.to_datetime(fatal_rows['date'], unit='s')
        
        grouped = fatal_rows.groupby('bundle_id').resample('1h', on='date').size()
        
        if (grouped > 10).any():
            return True
            
        return False