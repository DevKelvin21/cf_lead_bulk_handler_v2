import functions_framework
import os
import base64
import random
import json
from google.cloud import bigquery, firestore
from datetime import datetime, timedelta

# Initialize clients
bq_client = bigquery.Client()
firestore_client = firestore.Client()

ENV_VAR_MSG = "Specified environment variable is not set."

CONFIG_COLLECTION = os.getenv("CONFIG_COLLECTION", ENV_VAR_MSG)
CONFIG_DOCUMENT = os.getenv("CONFIG_DOCUMENT", ENV_VAR_MSG)

DISPOSITION_MAPPING = {
    "A": "Answering Machine",
    "AA": "Answering Machine Auto",
    "AB": "Busy Auto",
    "DC": "Disconnected Number",
    "N": "No Answer",
    "NAU": "No Answer",
    "NA": "No Answer Autodial",
}


def get_config_from_firestore():
    """Fetch configuration from Firestore."""
    doc_ref = firestore_client.collection(CONFIG_COLLECTION).document(CONFIG_DOCUMENT)
    doc = doc_ref.get()
    
    if not doc.exists:
        raise Exception(f"Configuration document '{CONFIG_DOCUMENT}' not found.")
    
    config = doc.to_dict()
    
    date_range_start_str = config.get('DATE_RANGE_START', '2023-01-01').strftime('%Y-%m-%d')
    date_range_start = datetime.strptime(date_range_start_str, '%Y-%m-%d')

    date_range_end_str = config.get('DATE_RANGE_END', '2023-12-31').strftime('%Y-%m-%d')
    date_range_end = datetime.strptime(date_range_end_str, '%Y-%m-%d')

    return {
        'records_per_lead_min': config.get('RECORDS_PER_LEAD_MIN', 1),
        'records_per_lead_max': config.get('RECORDS_PER_LEAD_MAX', 5),
        'date_range_start': date_range_start,
        'date_range_end': date_range_end,
        'bq_dataset_name': config.get('BQ_DATASET_NAME', 'your_dataset'),
        'bq_table_name': config.get('BQ_TABLE_NAME', 'call_records'),
        'listID': config.get('LIST_ID', 'DefaultListID'),
        'ClientName': config.get('CLIENT_NAME', 'DefaultClientName'),
        'campaign': config.get('CAMPAIGN', 'DefaultCampaign'),
    }


def random_date(start_date, end_date):
    """Generate a random weekday date within the provided range."""
    delta_days = (end_date - start_date).days
    random_day = start_date + timedelta(days=random.randint(0, delta_days))
    
    # Make sure the date is a weekday (Monday to Friday)
    while random_day.weekday() > 4:  # 0=Monday, 4=Friday
        random_day = start_date + timedelta(days=random.randint(0, delta_days))
    
    return random_day


def generate_random_calls(lead, num_calls, config):
    """Generate random call records for a given lead."""
    calls = []
    for _ in range(num_calls):
        # Generate random date and ensure it's a weekday
        call_date = random_date(config['date_range_start'], config['date_range_end'])
        talk_time = random.randint(30, 600)  # Talk time in seconds
        disposition_abbr = random.choice(list(DISPOSITION_MAPPING.keys()))
        disposition = DISPOSITION_MAPPING[disposition_abbr]
        term_reason = random.choice(['Customer Ended', 'Agent Ended', 'Timeout'])
        
        Address = f"{lead.get('Address', '')}, {lead.get('City', '')}, {lead.get('State', '')}, {lead.get('Zip', '')}"
        # Format call data
        call = {
            "Date": call_date.strftime('%Y-%m-%d %H:%M:%S'),
            "FirstName": lead.get('FirstName', 'Unknown'),
            "LastName": lead.get('LastName', ''),
            "CallNotesFormatted": f"Call with {lead.get('FirstName', 'Unknown')} {lead.get('LastName', 'Unknown')}",
            "Phone": lead.get('Phone', 'Unknown'),
            "Email": lead.get('Email', ''),
            "ListID": config['listID'],  # Use ListID from Firestore config
            "Disposition": disposition,  # Full word from the mapping
            "LeadID": lead.get('LeadID', 0),
            "TalkTimeFormatted": config['campaign'],  # Use Campaign from Firestore config
            "TermReasonFormatted": term_reason,
            "SubscriberIDFormatted": lead.get('SubscriberIDFormatted', 'Unknown'),
            "ListDescriptionFormatted": config['ClientName'],  # Use ClientName from Firestore config
            "Source": lead.get('Source', ''),
            "LeadType": lead.get('LeadType', ''),
            "Address": Address,
        }
        calls.append(call)
    return calls


def upload_to_bigquery(records, bq_dataset_name, bq_table_name):
    """Upload the generated call records to BigQuery."""
    table_id = f"{bq_dataset_name}.{bq_table_name}"
    errors = bq_client.insert_rows_json(table_id, records)
    if errors:
        raise Exception(f"Error inserting rows into BigQuery: {errors}")
    return f"Successfully inserted {len(records)} rows."


@functions_framework.cloud_event
def process_row(cloud_event):
    """Triggered by Pub/Sub. Processes a single lead row and inserts call records into BigQuery."""
    lead_data = base64.b64decode(cloud_event.data['message']['data']).decode('utf-8')
    lead = json.loads(lead_data)
    config = get_config_from_firestore()
    
    # Determine the number of calls to generate
    num_calls = random.randint(config['records_per_lead_min'], config['records_per_lead_max'])
    
    # Generate call records
    call_records = generate_random_calls(lead, num_calls, config)
    
    # Upload to BigQuery
    upload_to_bigquery(call_records, config['bq_dataset_name'], config['bq_table_name'])
    
    return f"Processed LeadID: {lead.get('LeadID', 'Unknown')}"
