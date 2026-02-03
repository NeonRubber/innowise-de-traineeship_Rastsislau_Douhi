# Metrics Service for bike data analysis using Pandas

import pandas as pd
import logging

logger = logging.getLogger()

class MetricsService:
    def calculate_daily_metrics(self, df: pd.DataFrame) -> list[dict]:
        # Validate and map required columns from dataset
        required_cols = ['departure_time', 'distance_m', 'duration_sec', 'avg_speed_kmh', 'air_temperature_c']

        # Extract date from available time columns
        if 'departure_time' in df.columns:
            df['date'] = pd.to_datetime(df['departure_time']).dt.date.astype(str)
        elif 'started_at' in df.columns:
            df['date'] = pd.to_datetime(df['started_at']).dt.date.astype(str)
        else:
            logger.warning("No date column found, skipping metrics")
            return []

        # Ensure numeric types
        numeric_cols = {
            'distance_m': ['distance', 'covered_distance_m'],
            'duration_sec': ['duration', 'duration_sec'],
            'avg_speed_kmh': ['speed', 'avg_speed'],
            'air_temperature_c': ['temperature', 'air_temperature']
        }
        
        # Map alternative column names to standard numeric fields
        for target, alternatives in numeric_cols.items():
            if target not in df.columns:
                for alt in alternatives:
                    if alt in df.columns:
                        df[target] = df[alt]
                        break
        
        # Calculate daily averages
        metrics = []
        
        # Group data by date to calculate daily metrics
        grouped = df.groupby('date')
        
        for date, group in grouped:
            daily_metric = {
                'date': date,
                'metric_type': 'daily_stats',
                'count': len(group)
            }
            
            if 'distance_m' in group.columns:
                daily_metric['avg_distance_m'] = group['distance_m'].mean()
            
            if 'duration_sec' in group.columns:
                daily_metric['avg_duration_sec'] = group['duration_sec'].mean()
                
            # Estimate speed if distance and duration are available
            if 'avg_distance_m' in daily_metric and 'avg_duration_sec' in daily_metric and daily_metric['avg_duration_sec'] > 0:
                 daily_metric['avg_speed_kmh'] = (daily_metric['avg_distance_m'] / daily_metric['avg_duration_sec']) * 3.6
            elif 'avg_speed_kmh' in group.columns:
                 daily_metric['avg_speed_kmh'] = group['avg_speed_kmh'].mean()
                 
            if 'air_temperature_c' in group.columns:
                daily_metric['avg_temperature_c'] = group['air_temperature_c'].mean()
                
            metrics.append(daily_metric)
            
        return metrics
        
    def calculate_monthly_metrics(self, df: pd.DataFrame) -> dict:
        # Calculate aggregate monthly summary
        
        if df.empty:
            return {}
            
        stats = {
            'metric_type': 'monthly_aggregate',
            'total_trips': len(df)
        }
        
        return stats
