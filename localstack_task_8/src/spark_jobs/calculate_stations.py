# PySpark job to calculate per-station bike metrics

import sys
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Configuration for the Station Metrics job
@dataclass
class JobConfig:
    input_path: str
    output_path: str
    departure_column: str = 'departure_name'
    
    # CSV column mappings for calculations
    dist_col: str = '`distance (m)`'
    dur_col: str = '`duration (sec.)`'
    speed_col: str = '`avg_speed (km/h)`'
    temp_col: str = '`Air temperature (degC)`'
    
    def validate(self) -> None:
        if not self.input_path:
            raise ValueError("input_path cannot be empty")
        if not self.output_path:
            raise ValueError("output_path cannot be empty")
        # Ensure output directory exists (local compatibility)
        try:
            Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass


# Abstract base class for reading data
class DataReader(ABC):
    @abstractmethod
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        pass


# CSV data reader implementation
class CsvDataReader(DataReader):
    def __init__(self, header: bool = True, infer_schema: bool = True):
        self.header = header
        self.infer_schema = infer_schema
    
    def read(self, spark: SparkSession, path: str) -> DataFrame:
        logger.info(f"Reading CSV from: {path}")
        
        df = spark.read.csv(
            path,
            header=self.header,
            inferSchema=self.infer_schema
        )
        
        return df


# Abstract base class for writing data
class DataWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, path: str) -> None:
        pass


# CSV data writer implementation
class CsvDataWriter(DataWriter):
    def __init__(self, mode: str = 'overwrite', header: bool = True):
        self.mode = mode
        self.header = header
    
    def write(self, df: DataFrame, path: str) -> None:
        logger.info(f"Writing results to: {path}")
        
        # Coalesce to single partition for single CSV output
        df.coalesce(1).write.csv(
            path,
            mode=self.mode,
            header=self.header
        )
        logger.info(f"Successfully wrote data to {path}")


# Calculate station metrics from bike trip data
class MetricsCalculator:
    def __init__(self, config: JobConfig):
        self.cfg = config
    
    def calculate(self, df: DataFrame) -> DataFrame:
        logger.info("Calculating per-station metrics")
        
        # Calculate aggregate metrics grouped by station
        
        result = df.groupBy(
            F.col(self.cfg.departure_column).alias("station_name")
        ).agg(
            F.count("*").alias("departure_count"),
            # Station-specific averages
            F.round(F.avg(self.cfg.dist_col), 2).alias("avg_distance_val"),
            F.round(F.avg(self.cfg.dur_col), 2).alias("avg_duration_val"),
            F.round(F.avg(self.cfg.speed_col), 2).alias("avg_speed_val"),
            F.round(F.avg(self.cfg.temp_col), 2).alias("avg_temp_val")
        )
        
        # Sort by popularity (highest trip count first)
        result = result.orderBy(F.col("departure_count").desc())
        
        logger.info(f"Calculated per-station metrics. Top rows preview:")
        result.show(5)
        
        return result


# Factory for creating and configuring SparkSession
class SparkSessionFactory:
    @staticmethod
    def create(app_name: str = "StationMetricsJob") -> SparkSession:
        logger.info(f"Creating Spark session: {app_name}")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark


# Main job class for calculating station metrics (ETL pipeline)
class StationMetricsJob:
    def __init__(
        self,
        config: JobConfig,
        reader: Optional[DataReader] = None,
        writer: Optional[DataWriter] = None,
        spark: Optional[SparkSession] = None
    ):
        config.validate()
        self.config = config
        self.reader = reader or CsvDataReader()
        self.writer = writer or CsvDataWriter()
        self.spark = spark or SparkSessionFactory.create()
        
        self.metrics_calculator = MetricsCalculator(config)
    
    def run(self) -> None:
        try:
            logger.info("="*60)
            logger.info("Starting Station Metrics Job")
            logger.info("="*60)
            
            input_df = self.reader.read(self.spark, self.config.input_path)
            metrics_df = self.metrics_calculator.calculate(input_df)
            self.writer.write(metrics_df, self.config.output_path)
            
            logger.info("="*60)
            logger.info("Job completed successfully")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Job failed: {e}")
            raise
        
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit calculate_stations.py <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    config = JobConfig(
        input_path=input_path,
        output_path=output_path
    )
    
    job = StationMetricsJob(config)
    job.run()


if __name__ == '__main__':
    main()