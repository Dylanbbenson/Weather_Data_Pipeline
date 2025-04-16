import subprocess
import retrieve_missing_dates_from_athena as missing
import time

dates = missing.get_missing_dates()

for date in dates:
    print(f"Fetching data for {date}")
    subprocess.run(["python3", "./src/retrieve_data.py", "--date", date])
    time.sleep(120) #wait 2 minutes for glue job to finish before next date
