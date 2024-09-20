import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from flask import Flask, request, send_file, jsonify
import openai

import psycopg2
import requests
from bs4 import BeautifulSoup
from flask import app, Flask
from flask_cors import CORS

from together import Together
app = Flask(__name__)
CORS(app)  # Aktivera CORS för hela appen

API_URL = "https://platsbanken-api.arbetsformedlingen.se/jobs/v1/search"
JOB_DETAIL_URL = "https://platsbanken-api.arbetsformedlingen.se/jobs/v1/job/"
MAX_RECORDS = 100
MAX_DETAILS = 2000

API_KEYS = [
    "be8a19eba4e0f34ee49a93d1feaeea5f853c4ac53c04d9c973c5e89e4b7bad4d",
    "2073310bc9a8fdb4b1050f3952d4818cff218f0cc8b4f53d0d66623813aa870e"
]

DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'test',
    'host': 'localhost',
    'port': '5432'
}

processed_count_lock = Lock()
processed_count = 0
failed_jobs = []
api_key_index = 0
api_key_lock = Lock()

# Skapa klienterna utanför funktionerna
clients = [Together(api_key=key) for key in API_KEYS]


def get_next_client():
    global api_key_index
    with api_key_lock:
        client = clients[api_key_index]
        api_key_index = (api_key_index + 1) % len(clients)
    return client


def create_tables(table_name):
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id VARCHAR(255) PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    published_date TIMESTAMP,
                    last_application_date TIMESTAMP,
                    number_of_positions INTEGER,
                    employment_type TEXT,
                    occupation TEXT,
                    company_name TEXT,
                    municipality TEXT,
                    country TEXT,
                    requires_experience BOOLEAN,
                    requires_license BOOLEAN,
                    requires_car BOOLEAN,
                    application_email TEXT,
                    application_url TEXT,
                    company_type TEXT,
                    processed BOOLEAN DEFAULT FALSE
                )
            ''')
            cur.execute('''
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='jobs' AND column_name='processed') THEN
                        ALTER TABLE jobs ADD COLUMN processed BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            ''')


def fetch_all_jobs(table_name):
    all_ads = []
    start_index = 0
    current_datetime = datetime.utcnow().isoformat() + 'Z'

    while True:
        payload = {
            "filters": [{"type": "freetext", "value": table_name}],
            "fromDate": None,
            "order": "date",
            "source": "pb",
            "startIndex": start_index,
            "toDate": current_datetime,
            "maxRecords": MAX_RECORDS
        }
        response = requests.post(API_URL, json=payload)

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()

        if 'ads' not in data:
            print(f"Unexpected response format: {data}")
            break

        ads = data['ads']
        if not ads:
            break

        all_ads.extend(ads)
        start_index += MAX_RECORDS

        print(f"Fetched {len(ads)} ads, total so far: {len(all_ads)}")

        if len(all_ads) >= MAX_DETAILS:
            break

        time.sleep(0.08)

    return all_ads[:MAX_DETAILS]


def process_job(job, process_id, table_name):
    global processed_count
    global failed_jobs

    job_id = job['id']
    client = get_next_client()
    print(f"Process {process_id}: Using API client for key index: {api_key_index}")

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT id FROM {table_name} WHERE id = %s AND processed = TRUE''', (job_id,))
                if cur.fetchone():
                    print(f"Process {process_id}: Job {job_id} already processed, skipping.")
                    return None

        job_details = fetch_job_details(job_id, process_id)
        if job_details:
            job_details = analyze_job_details(job_details, client, process_id)
            insert_job_to_db(job_details, process_id, table_name)
            with processed_count_lock:
                processed_count += 1
            return job_id
    except Exception as e:
        print(f"Process {process_id}: Error processing job {job_id}: {str(e)}")
        return f"failed:{job_id}"
    return None


def fetch_job_details(job_id, process_id):
    try:
        response = requests.get(JOB_DETAIL_URL + str(job_id))
        response.raise_for_status()
        job_details = response.json()
        print(f"Process {process_id}: Fetched details for job ID {job_id}")
        return job_details
    except requests.RequestException as e:
        print(f"Process {process_id}: Error fetching job details for job ID {job_id}: {str(e)}")
        return None


def analyze_job_details(job_details, client, process_id):
    try:
        job_id = job_details['id']
        time.sleep(1)
        if 'requiresExperience' in job_details and not job_details['requiresExperience']:
            job_details['requiresExperience'] = True
        ### verify_experience_requirement(job_details.get('description', ''),client, process_id) ##använda sen i prod
        description = job_details.get('description', '')
        company_name = job_details.get('company', {}).get('name', '')
        #determine_company_type(description, company_name, client, process_id, job_id) ##använda sen i prod
        job_details['companyType'] = "privat"

        return job_details
    except Exception as e:
        print(f"Process {process_id}: Error analyzing job details for job ID {job_id}: {str(e)}")
        return job_details


def determine_company_type(description, company_name, client, process_id, job_id):
    time.sleep(0.5)

    response = client.chat.completions.create(
        model="google/gemma-2-9b-it",
        messages=[
            {
                "role": "user",
                "content": f'''Beskrivning: {description}
                Annonsen ägs av : {company_name}
                Analysera denna och berätta om vad är typ för företag som äger annonsen? är det myndighet? kommun? eller privat företag? svara på svenska kortfattat. Endast exempel annonsen är från "privat" eller annonsen är från myndighet eller annonsen är från kommun'''
            }
        ],
        max_tokens=200,
        temperature=0.01
    )

    response_content = response.choices[0].message.content
    print(f"Process {process_id}: Företagstyp respons för annons {job_id}: {response_content}")

    if "myndighet" in response_content.lower():
        return "Myndighet"
    elif "kommun" in response_content.lower():
        return "Kommun"
    elif "privat" in response_content.lower():
        return "Privat"
    else:
        return "Okänt"


def verify_experience_requirement(description, client, process_id):
    while True:
        try:
            response = client.chat.completions.create(
                model="google/gemma-2-9b-it",
                messages=[
                    {
                        "role": "user",
                        "content": f'''{description}
                        Analysera denna och avgör om absolut kräver erfarenhet från jobbet eller inte. Om de kräver istället erfarenheter från skolan dvs utbildning då de kräver inte erfarenheter.
                        Skala från 1 till 10. om det är över 5 då är ja de kräver erfarenheter.
                        Svara ja eller nej? Analysera så noggrant och verifiera ditt svar innan du skickar. Svara kortfattat.'''
                    }
                ],
                max_tokens=200,
                temperature=0.01
            )

            response_content = response.choices[0].message.content

            print(f"Process {process_id}: {response_content}")
            return "Ja" in response_content
        except Exception as e:
            print(f"Process {process_id}: Error: {e}")
            if 'rate limit' in str(e).lower():
                print(f"Process {process_id}: Rate limit exceeded, waiting...")
                time.sleep(10)


def clean_html(text):
    if text is None:
        return text
    soup = BeautifulSoup(text, "html.parser")
    clean_text = soup.get_text(separator=" ", strip=True)
    return clean_text.replace('\n', ' ')


def insert_job_to_db(job, process_id, table_name):
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                municipality = job['workplace']['municipality']
                if municipality is None:
                    municipality = "distans"

                cur.execute(f'''
                    INSERT INTO {table_name} (
                        id, title, description, published_date, last_application_date,
                        number_of_positions, employment_type, occupation, company_name,
                        municipality, country, requires_experience, requires_license,
                        requires_car, application_email, application_url, company_type, processed
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        published_date = EXCLUDED.published_date,
                        last_application_date = EXCLUDED.last_application_date,
                        number_of_positions = EXCLUDED.number_of_positions,
                        employment_type = EXCLUDED.employment_type,
                        occupation = EXCLUDED.occupation,
                        company_name = EXCLUDED.company_name,
                        municipality = EXCLUDED.municipality,
                        country = EXCLUDED.country,
                        requires_experience = EXCLUDED.requires_experience,
                        requires_license = EXCLUDED.requires_license,
                        requires_car = EXCLUDED.requires_car,
                        application_email = EXCLUDED.application_email,
                        application_url = EXCLUDED.application_url,
                        company_type = EXCLUDED.company_type,
                        processed = TRUE
                ''', (
                    job['id'],
                    job['title'],
                    clean_html(job['description']),
                    job['publishedDate'],
                    job['lastApplicationDate'],
                    job['positions'],
                    job['employmentType'],
                    job['occupation'],
                    job['company']['name'],
                    municipality,  # Använd det uppdaterade värdet för municipality
                    job['workplace']['country'],
                    job['requiresExperience'],
                    bool(job.get('license')),
                    job.get('ownCar', False),
                    job['application'].get('email'),
                    job['application'].get('url'),
                    job['companyType']
                ))
                print(f"Process {process_id}: Job {job['id']} inserted/updated in the database.")
    except Exception as e:
        print(f"Process {process_id}: Error inserting job {job['id']} to database: {str(e)}")
        raise


def retry_failed_jobs(jobs, job_ads,table_name, max_retries=5):
    for retry in range(max_retries):
        print(f"\nRetry attempt {retry + 1}/{max_retries} for failed jobs...")
        retry_futures = []
        new_failed_jobs = []

        with ProcessPoolExecutor(max_workers=2) as retry_executor:
            for i, job_id in enumerate(jobs):
                process_id = i % 2 + 1
                job = next((j for j in job_ads if j['id'] == job_id), None)
                if job:
                    print(f"Retrying process {process_id} for job {job_id}")
                    retry_futures.append(retry_executor.submit(process_job, job, process_id, table_name))

            for future in as_completed(retry_futures):
                try:
                    result = future.result()
                    if result and not result.startswith("failed:"):
                        print(f"Successfully processed previously failed job {result}")
                    elif result and result.startswith("failed:"):
                        new_failed_jobs.append(result.split(":")[1])
                except Exception as e:
                    print(f"Error processing job in retry: {str(e)}")

        jobs = new_failed_jobs
        if not jobs:
            break

    return jobs


def fetch_jobs(table_name):
    def default_converter(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name} WHERE processed = TRUE")  # Använd tabellnamnet i SQL-frågan
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            jobs = [dict(zip(colnames, row)) for row in rows]

    json_filename = f'cleaned_detailed_job_ads_{table_name}.json'  # Använd tabellnamnet i filnamnet

    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, ensure_ascii=False, indent=4, default=default_converter)



@app.route('/process_jobs', methods=['POST'])
def process_jobs():
    data = request.json
    table_name = data.get('table_name')
    if not table_name:
        return jsonify({"error": "Both 'table_name' and 'search_term' are required"}), 400
    global failed_jobs

    create_tables(table_name)
    job_ads = fetch_all_jobs(table_name)

    print(f'Total ads fetched: {len(job_ads)}')

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = []
        for i, job in enumerate(job_ads):
            process_id = i % 2 + 1
            print(f"Starting process {process_id} for job {job['id']}")
            futures.append(executor.submit(process_job, job, process_id, table_name))

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    if result.startswith("failed:"):
                        failed_jobs.append(result.split(":")[1])
                    else:
                        print(f"Processed job {result}")
            except Exception as e:
                print(f"Error processing job: {str(e)}")

    print(f'Total jobs processed and saved in the database: {processed_count}')

    if failed_jobs:
        failed_jobs = retry_failed_jobs(failed_jobs, job_ads, table_name)

    if failed_jobs:
        print(f"Some jobs failed after retries: {failed_jobs}")
    else:
        print("All jobs processed successfully after retries.")

    print(f'Final total jobs processed and saved in the database: {processed_count}')

    fetch_jobs(table_name)
    # Return a valid JSON response indicating success and results
    return jsonify({
        "message": "Job processing completed",
        "total_jobs_fetched": len(job_ads),
        "total_jobs_processed": processed_count,
        "failed_jobs": failed_jobs,
        "json_filename": "cleaned_detailed_job_ads_"+table_name # Lägg till detta
    })

if __name__ == "__main__":
    app.run(debug=True)
