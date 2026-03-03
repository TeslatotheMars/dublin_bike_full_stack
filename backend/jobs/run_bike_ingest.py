import os
from api.bike import BikeRepository

def main():
    repo = BikeRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db"),
        jcdecaux_api_key=os.getenv("JCDECAUX_API_KEY"),
        jcdecaux_contract=os.getenv("JCDECAUX_CONTRACT", "dublin"),
        retention_days=int(os.getenv("BIKE_HISTORY_RETENTION_DAYS", "7")),
        create_partitions_ahead_days=int(os.getenv("BIKE_PARTITIONS_AHEAD_DAYS", "1")),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "15")),
    )
    # run once (use cron/Task Scheduler to run every 5 minutes)
    repo.run_once()


if __name__ == "__main__":
    main()
