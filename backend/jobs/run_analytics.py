import os
import logging
from api.analytics import AnalyticsRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting analytics pipeline...")
    
    repo = AnalyticsRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1").strip(),
        mysql_user=os.getenv("MYSQL_USER", "root").strip(),
        mysql_password=os.getenv("MYSQL_PASSWORD", "").strip(),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db").strip(),
    )

    try:
        # 1) stats
        stats_days = int(os.getenv("STATS_LOOKBACK_DAYS", "7"))
        repo.refresh_stats(lookback_days=stats_days)
        logger.info(f"✅ Stats table refreshed (lookback_days={stats_days})")

        # 2) train model with demo data fallback
        ml_days = int(os.getenv("ML_LOOKBACK_DAYS", "7"))
        include_fe = (os.getenv("INCLUDE_STATION_FE", "1") == "1")
        model_path = os.getenv("MODEL_PATH", "bike_availability_model.joblib")
        alpha = float(os.getenv("RIDGE_ALPHA", "1.0"))
        use_demo = (os.getenv("USE_DEMO_FALLBACK", "1") == "1")

        bundle = repo.train_and_save_model_all_data(
            lookback_days=ml_days,
            model_path=model_path,
            include_station_fe=include_fe,
            alpha=alpha,
            use_demo_fallback=use_demo,
        )
        logger.info(f"✅ Model training complete ({bundle['n_rows']} rows)")

        # 3) forecast
        forecast_hours = int(os.getenv("FORECAST_HOURS", "24"))
        forecast_step = int(os.getenv("FORECAST_STEP_MINUTES", "5"))
        
        written = repo.predict_and_store_bike_forecast(
            model_path=model_path,
            next_hours=forecast_hours,
            step_minutes=forecast_step,
        )
        logger.info(f"✅ Forecast complete ({written} rows written)")
        logger.info("✅ Analytics pipeline finished successfully!")
        
    except Exception as e:
        logger.error(f"❌ Analytics pipeline failed: {e}", exc_info=True)
        raise
    finally:
        repo.close()
        logger.info("Repository connection closed")


if __name__ == "__main__":
    main()
