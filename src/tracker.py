import datetime
import psycopg2

class Tracker(object):
    """Job tracker with parallel-run prevention and dependency checks"""

    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig

    def assign_job_id(self):
        today = datetime.date.today().strftime("%Y-%m-%d")
        return f"{self.jobname}_{today}"

    def update_job_status(self, status):
        job_id = self.assign_job_id()
        update_time = datetime.datetime.now()
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {table_name} (job_id, status, updated_time)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (job_id)
                    DO UPDATE SET status = EXCLUDED.status, updated_time = EXCLUDED.updated_time;
                """, (job_id, status, update_time))
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print("error executing db statement for job tracker.", error)
        finally:
            connection.close()

    def is_job_running(self):
        """Prevent multiple parallel runs of the same job."""
        job_id = self.assign_job_id()
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT status FROM {table_name} WHERE job_id = %s
                """, (job_id,))
                row = cursor.fetchone()
                if row and row[0] == "running":
                    return True
        finally:
            connection.close()
        return False

    def get_job_status(self, jobname, date=None):
        """Check ingestion status before running analytics"""
        if not date:
            date = datetime.date.today().strftime("%Y-%m-%d")
        job_id = f"{jobname}_{date}"
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT status FROM {table_name} WHERE job_id = %s", (job_id,))
                row = cursor.fetchone()
                return row[0] if row else None
        finally:
            connection.close()

    def get_db_connection(self):
        return psycopg2.connect(
            user=self.dbconfig.get("postgres", "user"),
            password=self.dbconfig.get("postgres", "password"),
            host=self.dbconfig.get("postgres", "host"),
            port=self.dbconfig.get("postgres", "port"),
            database=self.dbconfig.get("postgres", "database"),
        )
