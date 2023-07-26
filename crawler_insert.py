import datetime
import argparse
import mysql.connector

parser = argparse.ArgumentParser()
parser.add_argument("-C", "--Crawler_name", help="Crawler to run", required=True)
parser.add_argument(
    "-Cd", "--Crawler_description", help="Crawler description", required=True
)
parser.add_argument("-Cl", "--location", help="location", required=True)
parser.add_argument("-Cf", "--frequency", help="frequency to run", default="daily")
parser.add_argument(
    "-Clr", "--last_run", help="last run time", default=datetime.datetime.now()
)
parser.add_argument(
    "-Cnr", "--next_run", help="next run time", default=datetime.datetime.now()
)
parser.add_argument("-Crt", "--runtime", help="total time to run", default=0)
parser.add_argument("-Crs", "--runstatus", help="Crawler run status", default=True)
parser.add_argument("-Cs", "--status", help="Crawler to run", default=False)
parser.add_argument("-Cr", "--records", help="product records", default=0)
parser.add_argument("-Ct", "--total_runs", help="total runs", default=0)
args = parser.parse_args()

with open("config.json", "r") as f:
    config = eval(f.read())
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
add_dsp = "INSERT INTO crawlers_info (crawler_name,crawler_discreption,location,frequency,last_run,next_run,runtime,runstatus,status,records,total_runs,created_at,updated_at) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s)"
cursor.execute(
    add_dsp,
    (
        args.Crawler_name,
        args.Crawler_description,
        args.location,
        args.frequency,
        args.last_run,
        args.next_run,
        args.runtime,
        args.runstatus,
        args.status,
        args.records,
        args.total_runs,
        str(datetime.datetime.now()),
        str(datetime.datetime.now()),
    ),
)
cnx.commit()
cursor.close()
cnx.close()
