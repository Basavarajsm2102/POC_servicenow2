from apscheduler.schedulers.blocking import BlockingScheduler
from main import main

def run_scheduled_job():
    """Run the main pipeline on a schedule."""
    print(f"Starting scheduled job at {datetime.now()}")
    try:
        main()
        print("Scheduled job completed successfully")
    except Exception as e:
        print(f"Scheduled job failed: {e}")

def schedule_pipeline():
    """Schedule the pipeline to run daily at 10 PM IST."""
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(run_scheduled_job, 'cron', hour=22, minute=0)
    print("Scheduler started. Running daily at 10 PM IST...")
    scheduler.start()

if __name__ == "__main__":
    schedule_pipeline()
