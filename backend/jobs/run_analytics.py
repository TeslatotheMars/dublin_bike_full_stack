from pathlib import Path
import os
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

print("Starting analytics pipeline...")
print("MYSQL_DB:", os.getenv("MYSQL_DB"))

from api.analytics import AnalyticsRepository


def main():
    repo = AnalyticsRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1").strip(),
        mysql_user=os.getenv("MYSQL_USER", "root").strip(),
        mysql_password=os.getenv("MYSQL_PASSWORD", "").strip(),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db").strip(),
    )

    try:
        stats_days = int(os.getenv("STATS_LOOKBACK_DAYS", "7"))
        print(f"\nStep 1: Refreshing stats (lookback={stats_days} days)...")
        repo.refresh_stats(lookback_days=stats_days)
        print(" Stats done")

        ml_days    = int(os.getenv("ML_LOOKBACK_DAYS", "7"))
        model_path = os.getenv("MODEL_PATH", "bike_availability_model.joblib")
        include_fe = (os.getenv("INCLUDE_STATION_FE", "1") == "1")
        alpha      = float(os.getenv("RIDGE_ALPHA", "1.0"))

        print(f"\nStep 2: Training ML model (lookback={ml_days} days)...")
        bundle = repo.train_and_save_model_all_data(
            lookback_days=ml_days, model_path=model_path,
            include_station_fe=include_fe, alpha=alpha,
        )
        print(f"✅ Model trained on {bundle['n_rows']} rows → {model_path}")

        forecast_hours = int(os.getenv("FORECAST_HOURS", "24"))
        forecast_step  = int(os.getenv("FORECAST_STEP_MINUTES", "5"))
        print(f"\nStep 3: Generating {forecast_hours}h forecasts...")
        written = repo.predict_and_store_bike_forecast(
            model_path=model_path, next_hours=forecast_hours, step_minutes=forecast_step,
        )
        print(f"✅ bike_forecast: {written} rows written")
        print("\n🎉 Analytics complete! Charts should now show in the app.")

    except RuntimeError as e:
        print(f"\n ERROR: {e}")
        print("Run bike ingest and weather ingest first, then retry.")
    finally:
        repo.close()


if __name__ == "__main__":
    main()