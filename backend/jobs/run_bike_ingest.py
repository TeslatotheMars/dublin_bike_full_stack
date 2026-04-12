from pathlib import Path
import os
from dotenv import load_dotenv

# Load .env from backend folder
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

from api.bike import BikeRepository


def main():
    repo = BikeRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1").strip(),
        mysql_user=os.getenv("MYSQL_USER", "root").strip(),
        mysql_password=os.getenv("MYSQL_PASSWORD", "").strip(),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306").strip()),
        db_name=os.getenv("MYSQL_DB", "bike_db").strip(),
        jcdecaux_api_key=os.getenv("JCDECAUX_API_KEY", "").strip(),
        jcdecaux_contract=os.getenv("JCDECAUX_CONTRACT", "dublin").strip(),
        retention_days=int(os.getenv("BIKE_HISTORY_RETENTION_DAYS", "7")),
        create_partitions_ahead_days=int(os.getenv("BIKE_PARTITIONS_AHEAD_DAYS", "1")),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "15")),
    )
    print("Running bike ingest...")
    repo.run_once()
    print("✅ Bike ingest complete!")


if __name__ == "__main__":
    main()