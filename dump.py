import os
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler



def dump_database():
    dump_folder = "dumps"
    if not os.path.exists(dump_folder):
        os.makedirs(dump_folder)

    dump_filename = f"{dump_folder}/dump_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql"

    dump_command = f"pg_dump {os.getenv('DATABASE_URL')} > {dump_filename}"

    os.system(dump_command)
    print(f"Database dumped to {dump_filename}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()

    # Установка времени для дампа
    scheduler.add_job(dump_database, 'cron', hour=os.getenv("DUMP_TIME").split(':')[0],
                      minute=os.getenv("DUMP_TIME").split(':')[1])

    scheduler.start()