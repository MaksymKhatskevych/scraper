import os
import shutil
from datetime import datetime
import schedule
import time
from dotenv import load_dotenv
from database import connect_db

load_dotenv()


def dump_database():
    try:
        conn = connect_db()
        db_url = os.getenv("DATABASE_URL")
        dump_folder = os.path.join(os.getcwd(), "dumps")

        # Create 'dump' folder if it doesn't exist
        if not os.path.exists(dump_folder):
            os.makedirs(dump_folder)

        dump_filename = f"database_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        dump_filepath = os.path.join(dump_folder, dump_filename)

        # Using pg_dump to dump the PostgreSQL database to a SQL file
        dump_command = f"pg_dump {db_url} > {dump_filepath}"
        os.system(dump_command)

        print(f"Database dumped to {dump_filepath}")

        conn.close()

        return dump_filepath

    except Exception as e:
        print(f"An error occurred during database dump: {str(e)}")
        return None


def daily_dump_job():
    dump_filepath = dump_database()
    if dump_filepath:
        destination_folder = os.path.join(os.getcwd(), "dumps")
        destination_path = os.path.join(destination_folder, os.path.basename(dump_filepath))
        shutil.copy2(dump_filepath, destination_path)
        print(f"Daily dump copied to {destination_path}")


def schedule_daily_dump():
    # Schedule daily dump at 12:00 PM
    schedule.every().day.at("12:00").do(daily_dump_job)


if __name__ == "__main__":
    # Run the daily dump job immediately (for testing purposes)
    daily_dump_job()

    # Keep the script running to allow scheduled tasks to execute
    while True:
        schedule.run_pending()
        time.sleep(1)
