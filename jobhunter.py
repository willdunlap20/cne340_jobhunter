import mysql.connector
import time
import json
import requests
from datetime import date
import html2text


# Connect to database
def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='',
                                   host='localhost', database='job_hunter')
    conn.autocommit = True
    return conn


# Create the table structure
def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (id INT PRIMARY KEY auto_increment, Job_id varchar(50), 
    company varchar(300), Created_at DATE, url varchar(30000), Title VARCHAR(500), Description TEXT); ''')


# Query the database.
# You should not need to edit anything in this function
def query_sql(cursor, query):
    cursor.execute(query)
    return cursor


# Add a new job
def add_new_job(cursor, jobdetails):
    description = html2text.html2text(jobdetails['description'])
    created_at = jobdetails['publication_date'][0:10]
    job_id = jobdetails['id']
    title = jobdetails['title']
    company = jobdetails['company_name']
    url = jobdetails['url']
    query = "INSERT INTO jobs (Job_id, Title, Company, Url, Description, Created_at) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (job_id, title, company, url, description, created_at))
    return cursor


# Check if new job
def check_if_job_exists(cursor, jobdetails):
    job_id = jobdetails['id']
    query = "SELECT 1 FROM jobs WHERE Job_id = %s"
    cursor.execute(query, (job_id,))
    return cursor.fetchone() is not None


# Deletes job
def delete_job(cursor, jobdetails):
    query = "DELETE FROM jobs WHERE Job_id = %s"
    return query_sql(cursor, query, (jobdetails["id"],))

# Grab new jobs from a website, Parses JSON code and inserts the data into a list of dictionaries do not need to edit
def fetch_new_jobs():
    query = requests.get("https://remotive.io/api/remote-jobs")
    datas = json.loads(query.text)
    return datas


# Main area of the code. Should not need to edit
def jobhunt(cursor):
    # Fetch jobs from website
    jobpage = fetch_new_jobs()  # Gets API website and holds the json data in it as a list
    add_or_delete_job(jobpage, cursor)


def add_or_delete_job(jobpage, cursor):
    # Add your code here to parse the job page
    for jobdetails in jobpage['jobs']:  # EXTRACTS EACH JOB FROM THE JOB LIST
        # Check if the job already exists in the DB
        if not check_if_job_exists(cursor, jobdetails):  # If the job does not exist
            add_new_job(cursor, jobdetails)
            print(f"New job added: {jobdetails['title']} at {jobdetails['company_name']}")
        else:
            print(f"Job already exists: {jobdetails['title']} at {jobdetails['company_name']}")

def remove_old_jobs(cursor, conn):
    cursor.execute("DELETE FROM jobs WHERE Created_at < CURDATE() - INTERVAL 14 DAY")
    conn.commit()  # Ensure changes are saved
    print(f"Removed {cursor.rowcount} old jobs")

# Setup portion of the program. Take arguments and set up the script
def main():
    # Connect to SQL and get cursor
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor)

    while True:  # Infinite loop for background scraping
        jobhunt(cursor)
        remove_old_jobs(cursor, conn)  # Call the cleanup function
        time.sleep(14400)  # Sleep for 6 hours (21600 seconds) to avoid hitting API limits


if __name__ == '__main__':
    main()