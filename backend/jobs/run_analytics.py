import os
from api.analytics import AnalyticsRepository


def main():
    repo = AnalyticsRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db"),
    )

    try:
        # 1) stats
        stats_days = int(os.getenv("STATS_LOOKBACK_DAYS", "7"))
        repo.refresh_stats(lookback_days=stats_days)

        # 2) train model
        ml_days = int(os.getenv("ML_LOOKBACK_DAYS", "7"))
        include_fe = (os.getenv("INCLUDE_STATION_FE", "1") == "1")
        model_path = os.getenv("MODEL_PATH", "bike_availability_model.joblib")
        alpha = float(os.getenv("RIDGE_ALPHA", "1.0"))

        repo.train_and_save_model_all_data(
            lookback_days=ml_days,
            model_path=model_path,
            include_station_fe=include_fe,
            alpha=alpha,
        )

        # 3) forecast
        forecast_hours = int(os.getenv("FORECAST_HOURS", "24"))
        forecast_step = int(os.getenv("FORECAST_STEP_MINUTES", "5"))
        repo.predict_and_store_bike_forecast(
            model_path=model_path,
            next_hours=forecast_hours,
            step_minutes=forecast_step,
        )
    finally:
        repo.close()


if __name__ == "__main__":
    main()
