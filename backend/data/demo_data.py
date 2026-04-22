"""
Demo data module for fallback scenarios when insufficient historical data.
Provides synthetic bike and weather data based on realistic Dublin patterns.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import math


def generate_demo_hourly_dataset(lookback_days: int = 7) -> pd.DataFrame:
    """
    Generate synthetic hourly dataset for training when real data is insufficient.
    
    This mimics:
    - Realistic Dublin bike usage patterns (low at night, peaks 8-10am and 5-7pm)
    - Weekly seasonality (weekdays busier than weekends)
    - Weather variation
    - Multiple stations with different behavior
    
    Args:
        lookback_days: Number of days of demo data to generate
    
    Returns:
        DataFrame with columns matching real hourly dataset:
        station_id, hour_ts, y_available_bikes, wind_speed, temperature, weather_main
    """
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=lookback_days)
    
    data = []
    
    # Dublin Bikes typically has ~40-50 stations
    station_ids = list(range(1, 46))
    
    current_time = start_time
    while current_time <= end_time:
        weekday = current_time.weekday()  # Monday=0, Sunday=6
        hour = current_time.hour
        
        for station_id in station_ids:
            # Base availability: 15 bikes average per station
            base_bikes = 15
            
            # Weekday effect: busier Mon-Fri
            weekday_factor = 1.0 if weekday < 5 else 0.85
            
            # Hourly pattern: low at night (0-6am), peaks during work commute
            if 0 <= hour < 6:
                hourly_factor = 0.3
            elif 6 <= hour < 9:  # Morning commute start
                hourly_factor = 1.3
            elif 9 <= hour < 17:  # Day time
                hourly_factor = 0.8 if weekday < 5 else 1.0
            elif 17 <= hour < 20:  # Evening commute
                hourly_factor = 1.4
            else:  # 20-23
                hourly_factor = 0.6
            
            # Station variance: some stations naturally busier
            station_factor = 0.8 + (station_id % 10) * 0.04
            
            # Random noise
            noise = np.random.normal(0, 2)
            
            available_bikes = max(0, int(
                base_bikes * weekday_factor * hourly_factor * station_factor + noise
            ))
            
            # Weather: realistic Dublin patterns
            # Temperature varies with hour, cooler at night
            temp = 12 + 8 * math.sin(2 * math.pi * (hour - 6) / 24)
            wind_speed = 8 + 3 * math.sin(2 * math.pi * current_time.timetuple().tm_yday / 365)
            
            # Rain probability: 40% of hours in Dublin
            weather_main = "Rain" if np.random.random() < 0.4 else "Clear"
            
            data.append({
                'station_id': station_id,
                'hour_ts': current_time,
                'y_available_bikes': available_bikes,
                'wind_speed': round(wind_speed, 2),
                'temperature': round(temp, 2),
                'weather_main': weather_main,
                'lag_1h': None  # Will be computed by analytics module
            })
        
        current_time += timedelta(hours=1)
    
    df = pd.DataFrame(data)
    df['hour_ts'] = pd.to_datetime(df['hour_ts'], utc=True)
    
    # Compute lag features
    df = df.sort_values(['station_id', 'hour_ts'])
    df['lag_1h'] = df.groupby('station_id')['y_available_bikes'].shift(1)
    df = df.dropna(subset=['lag_1h'])
    
    return df


def generate_demo_stats_data(lookback_days: int = 7) -> pd.DataFrame:
    """
    Generate demo statistics data for each station/day/hour combination.
    
    Returns:
        DataFrame with columns: station_id, day, hour, average
    """
    
    data = []
    station_ids = list(range(1, 46))
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    for station_id in station_ids:
        for day_idx, day_name in enumerate(days):
            for hour in range(24):
                # Base availability
                base_bikes = 15
                
                # Day effect: weekdays busier
                day_factor = 1.0 if day_idx < 5 else 0.85
                
                # Hour effect
                if 0 <= hour < 6:
                    hourly_factor = 0.3
                elif 6 <= hour < 9:
                    hourly_factor = 1.3
                elif 9 <= hour < 17:
                    hourly_factor = 0.8 if day_idx < 5 else 1.0
                elif 17 <= hour < 20:
                    hourly_factor = 1.4
                else:
                    hourly_factor = 0.6
                
                # Station variance
                station_factor = 0.8 + (station_id % 10) * 0.04
                
                average = max(0, base_bikes * day_factor * hourly_factor * station_factor)
                
                data.append({
                    'station_id': station_id,
                    'day': day_name,
                    'hour': hour,
                    'average': round(average, 2)
                })
    
    return pd.DataFrame(data)


if __name__ == "__main__":
    # Test the demo data generation
    print("Generating demo hourly dataset...")
    df_hourly = generate_demo_hourly_dataset(lookback_days=7)
    print(f"Generated {len(df_hourly)} rows")
    print(df_hourly.head(10))
    
    print("\nGenerating demo stats data...")
    df_stats = generate_demo_stats_data()
    print(f"Generated {len(df_stats)} rows")
    print(df_stats.head(10))
