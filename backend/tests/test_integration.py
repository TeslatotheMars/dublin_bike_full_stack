import pytest
import os
from datetime import datetime, timezone

# Import the main entry point from each job script
from jobs.run_bike_ingest import main as main_bike
from jobs.run_weather_ingest import main as main_weather
from jobs.run_analytics import main as main_analytics
from api.db import get_db_conn


@pytest.mark.integration
def test_full_pipeline():
    """
    Runs the full data pipeline (ingest and analytics) against the database
    and verifies the output. This is a long-running test.
    """
    # Set environment variables for a faster, more constrained test run
    os.environ["STATS_LOOKBACK_DAYS"] = "1"
    # Shorter lookback for faster training
    os.environ["ML_LOOKBACK_DAYS"] = "7"
    os.environ["FORECAST_HOURS"] = "24"
    os.environ["RETENTION_DAYS"] = "1"

    # NOTE: This test runs against the DB configured in your .env file.
    # For a real CI/CD pipeline, you would point this to a dedicated test database.

    try:
        # Run all 3 job scripts in sequence
        print("\n[Pipeline] Running bike ingest job...")
        main_bike()
        print("[Pipeline] Running weather ingest job...")
        main_weather()
        print("[Pipeline] Running analytics and forecasting job...")
        main_analytics()
        print("[Pipeline] All jobs completed successfully.")

    except Exception as e:
        # If any part of the pipeline fails, the entire integration test fails.
        pytest.fail(
            f"Data pipeline integration test crashed during execution: {e}")

    # Connect to the database to verify the results
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            # Assertion 1: Check if bike_forecast table has new data.
            cursor.execute("SELECT COUNT(*) as count FROM bike_forecast")
            result = cursor.fetchone()
            count = result['count'] if isinstance(result, dict) else result[0]
            assert count > 0, "Integration Test Failed: No forecast data was generated in the bike_forecast table."

            # Assertion 2: Check if the latest forecast is in the future.
            cursor.execute(
                "SELECT MAX(forecast_time) as max_time FROM bike_forecast")
            result = cursor.fetchone()
            max_time = result['max_time'] if isinstance(
                result, dict) else result[0]

            # The database returns a naive datetime object (assumed to be in UTC).
            # We compare it against the current UTC time.
            now_utc = datetime.utcnow()
            assert max_time > now_utc, \
                f"Integration Test Failed: Latest forecast time {max_time} is not in the future (current UTC: {now_utc})."
    finally:
        conn.close()
