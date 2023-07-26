import datetime
import mysql.connector
import subprocess

# import pdb;pdb.set_trace()
get_id = "Select crawler_name,next_run,status from crawlers_info"
with open("config.json", "r") as f:
    config = eval(f.read())
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor(buffered=True)
cursor.execute(get_id)
values = cursor.fetchall()
for value in values:
    if int(value[2]):
        if datetime.datetime.now().strftime("%H") == value[1].strftime("%H"):
            cmd = "scrapy crawl " + value[0]
            subprocess.call(cmd, shell=True)
