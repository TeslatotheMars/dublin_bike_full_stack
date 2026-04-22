"""
Data availability checker - determines whether to use real or demo data.
"""

import pymysql
from datetime import datetime, timedelta
from typing import Tuple


def check_data_availability(
    mysql_host: str,
    mysql_user: str,
    mysql_password: str,
    mysql_port: int = 3306,
    db_name: str = "bike_db",
    min_lookback_days: int = 7
) -> Tuple[bool, str]:
    """
    Check if database has sufficient data for model training.
    
    Args:
        mysql_host: MySQL host
        mysql_user: MySQL user
        mysql_password: MySQL password
        mysql_port: MySQL port
        db_name: Database name
        min_lookback_days: Minimum days of data required
    
    Returns:
        Tuple of (has_sufficient_data: bool, reason: str)
    """
    
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            port=int(mysql_port),
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        
        with conn.cursor() as cursor:
            # Check bike_history table exists and has data
            cursor.execute("SHOW TABLES LIKE 'bike_history'")
            if not cursor.fetchone():
                conn.close()
                return False, "bike_history table does not exist"
            
            # Count rows in bike_history from last N days
            cursor.execute("""
                SELECT COUNT(*) as cnt, 
                       MIN(scraped_at) as earliest,
                       MAX(scraped_at) as latest
                FROM bike_history
                WHERE scraped_at >= (UTC_TIMESTAMP() - INTERVAL %s DAY)
            """, (min_lookback_days,))
            
            result = cursor.fetchone()
            row_count = result['cnt'] if result else 0
            earliest = result['earliest'] if result else None
            latest = result['latest'] if result else None
            
            conn.close()
            
            # Criteria for sufficient data:
            # 1. At least 50k rows (reasonable coverage for 45 stations * 7 days * 24 hours ~ 7560 rows minimum)
            # 2. Data spans recent time
            # 3. Latest data is within last 24 hours
            
            if row_count < 5000:
                return False, f"Insufficient bike_history data: {row_count} rows (need >5000)"
            
            if latest:
                hours_since_latest = (datetime.utcnow() - latest.replace(tzinfo=None)).total_seconds() / 3600
                if hours_since_latest > 48:
                    return False, f"Latest bike data is {hours_since_latest:.1f} hours old (needs <48h)"
            
            if earliest:
                age_days = (datetime.utcnow() - earliest.replace(tzinfo=None)).total_seconds() / 86400
                if age_days < min_lookback_days:
                    return False, f"Data only spans {age_days:.1f} days (needs >= {min_lookback_days})"
            
            return True, f"Sufficient data: {row_count} rows spanning {age_days:.1f} days"
        
    except Exception as e:
        return False, f"Database check failed: {str(e)}"


def get_data_quality_summary(
    mysql_host: str,
    mysql_user: str,
    mysql_password: str,
    mysql_port: int = 3306,
    db_name: str = "bike_db",
) -> dict:
    """
    Get detailed data quality metrics.
    
    Returns:
        Dictionary with metrics like row counts, date range, station count, etc.
    """
    
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            port=int(mysql_port),
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        
        with conn.cursor() as cursor:
            # Bike data stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT station_id) as unique_stations,
                    MIN(scraped_at) as earliest_time,
                    MAX(scraped_at) as latest_time,
                    AVG(available_bikes) as avg_bikes
                FROM bike_history
            """)
            bike_stats = cursor.fetchone()
            
            # Weather data stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    MIN(observed_at) as earliest_time,
                    MAX(observed_at) as latest_time
                FROM weather_history
            """)
            weather_stats = cursor.fetchone()
            
            conn.close()
            
            return {
                'bike_data': {
                    'total_rows': bike_stats['total_rows'] if bike_stats else 0,
                    'unique_stations': bike_stats['unique_stations'] if bike_stats else 0,
                    'earliest': bike_stats['earliest_time'].isoformat() if bike_stats and bike_stats['earliest_time'] else None,
                    'latest': bike_stats['latest_time'].isoformat() if bike_stats and bike_stats['latest_time'] else None,
                    'avg_available': round(bike_stats['avg_bikes'], 2) if bike_stats and bike_stats['avg_bikes'] else 0,
                },
                'weather_data': {
                    'total_rows': weather_stats['total_rows'] if weather_stats else 0,
                    'earliest': weather_stats['earliest_time'].isoformat() if weather_stats and weather_stats['earliest_time'] else None,
                    'latest': weather_stats['latest_time'].isoformat() if weather_stats and weather_stats['latest_time'] else None,
                }
            }
    
    except Exception as e:
        return {'error': str(e)}
