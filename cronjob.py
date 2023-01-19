from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from componentdb_dump import main_componentdb_dump
from auto_mail import main_auto_mail, history_auto_mail
from gmail_attachment import main_gmail_attachments


def execute():
    # print(f"Crawling at {datetime.now()}.")
    try:
        main_gmail_attachments()
        main_componentdb_dump()
    except:
        print("Mail not found")


scheduler = BlockingScheduler()
scheduler.add_job(
    func=execute,
    trigger=CronTrigger.from_crontab(expr="00 13,14 * * *")
)
scheduler.add_job(
    func = main_auto_mail,
    trigger=CronTrigger.from_crontab(expr="00 15,16 * * *")
)

scheduler.add_job(
    func = history_auto_mail,
    trigger=CronTrigger.from_crontab(expr="00 15,16 * * *"),
)

try:
    execute()
except Exception as e:
    print(e)
    
try:
    main_auto_mail()
except Exception as e:
    print(e)

print("Cronjob will be run at 13:00 and 15:00 everyday.")
scheduler.start()

