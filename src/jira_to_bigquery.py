import os
import re
import logging
import argparse
import pytz

from jira import JIRA
import unicodedata
from datetime import datetime

# GCP
from google.oauth2 import service_account
from google.cloud import bigquery
import google.auth
import google.cloud.logging


#----------------------------
# Context
#----------------------------

JIRA_URL = "https://jira.gouv.nc"

# traité par argparse
# jira_project = "" # ex PSPC
# gcp_project = "" # ex prj-davar-p-bq-a01c
# bq_dataset = "" # ex pspc
# jira_token = ""

CRED_FILE = '' # ex: prj-davar-p-bq-a01c-c04f4c56dc89.json

jira_api_date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+1100")

#----------------------------
# auth & logging
#----------------------------

ENV_LOCAL = os.path.exists(CRED_FILE)
# Authenticate google drive and bigquery APIs using credentials.
SCOPES = [ 'https://www.googleapis.com/auth/bigquery']

try:
    if ENV_LOCAL:
        logging.basicConfig(level=logging.DEBUG)
        creds = service_account.Credentials.from_service_account_file(CRED_FILE)
    else:
        logging.basicConfig(level=logging.INFO)
        log_client = google.cloud.logging.Client()
        log_client.setup_logging()

        creds, _ = google.auth.default()

    # retrive credentials for scopes defined. google.auth.default() doesn't support methode if exex in an env without serv account
    scoped_credentials = creds.with_scopes(SCOPES)

except Exception as e:
    logging.error(f"Échec de l'authentification : {e}")
    raise

#----------------------------
# Fonctions
#----------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(description="Synchronization Jira to BigQuery")

    parser.add_argument(
        '--jira-project',
        type=str,
        required=True,
        help='Key of Jira project (ex : DATA)'
        )

    parser.add_argument(
        '--gcp-project',
        type=str,
        required=True,
        help='name of GCP project'
        )

    parser.add_argument(
        '--bq-dataset',
        type=str,
        required=True,
        help='name of BigQuery dataset'
        )
    
    parser.add_argument(
        '--jira-token',
        type=str,
        required=True,
        help='jira token for service account'
        )
    
    parser.add_argument(
        '--exclusion-fields',
        type=str,
        default="",
        help='fields to exclude'
        )
    
    parser.add_argument(
        '--exclusion-types',
        type=str,
        default="",
        help='issue types to exclude'
        )
    
    args = parser.parse_args() # transforme les - en _
    return args

def remove_accents(input_str):
    # Décomposer les caractères Unicode en leurs composants de base
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    # Filtrer les caractères de type "Mn" (marques non espacées, c'est-à-dire les accents)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def to_bigquery_name(field_name):
    # Enlever les accents
    field_name = remove_accents(field_name)
    # Remplacer les caractères non alphanumériques par des underscores
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', field_name)
    # S'assurer que le nom commence par une lettre ou un underscore
    if not clean_name[0].isalpha() and clean_name[0] != '_':
        clean_name = '_' + clean_name
    # Limiter la longueur du nom à 128 caractères
    return clean_name[:128]

def get_fields_map(jira):
    # map des custom fields avec les noms affichés
    jira_fields = jira.fields()
    # field_mapping = {field['name']: field['id'] for field in jira_fields}
    field_mapping = {field['id']: to_bigquery_name(field['name']) for field in jira_fields}
    return field_mapping

def fetch_all_issues(client, jql_query, max_results=100):
    logging.info('Beging of issues harvesting')
    all_issues = []
    start_at = 0

    while True:

        issues = client.search_issues(jql_query, startAt=start_at, maxResults=max_results, fields='*all', json_result=True, expand='changelog')
        if len(issues['issues']) == 0:
            break

        issues = [{"jira": issue['key'], "id": issue['id'], **issue['fields'], "changelog": issue['changelog']}  for issue in issues['issues']]

        all_issues.extend(issues)

        start_at += max_results
        if len(all_issues) % 5000 == 0:
            logging.info(f'{len(all_issues)} issues harvested.')

    logging.info(f'En of issues harvest. Total : {len(all_issues)} issues harvested.')

    return all_issues

def cast_value(value):
    if isinstance(value, list):
        value = ', '.join([cast_value(v) for v in value])
    else:
        value = str(value)
    return value


def load_to_bq(client, issues, table_name, gcp_project, bq_dataset): 

    destination = f"{gcp_project}.{bq_dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",
                                        autodetect = True
                                        )   
    job = client.load_table_from_json(issues, destination, job_config=job_config)

    job.result()  # Attendre la fin du job

    logging.info("Data for issue type '%s' have been insered successfully.", table_name)

def convert_to_utc(date_str):
    """Convertit une date ISO 8601 en UTC"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        dt_utc = dt.astimezone(pytz.utc)  # Convertir en UTC
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f")  # Format BigQuery
    except ValueError:
        return date_str

def traverse_and_convert(data, pattern):
    """Parcourt un JSON et convertit toutes les dates détectées en UTC"""
    if isinstance(data, dict):
        return {key: traverse_and_convert(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [traverse_and_convert(item) for item in data]
    elif isinstance(data, str) and pattern.match(data):
        return convert_to_utc(data)
    else:
        return data  


def format_issues(issues, field_mapping, exclusion_fields=[]):

    issues = [{field_mapping.get(k, k): v for k, v in item.items()} for item in issues]

    if len(exclusion_fields) != 0:
        issues = [{k: v for k, v in item.items() if k not in exclusion_fields} for item in issues]

    return issues

def clean_empty_structs(data):
    """Clean the empty structures in a list of dict (for BQ load)."""
    if isinstance(data, list):
        return [clean_empty_structs(item) for item in data]
    elif isinstance(data, dict):
        return {k: clean_empty_structs(v) if v != {} else None for k, v in data.items()}
    else:
        return data

def merge_tables(bq_client, target, source, gcp_project, bq_dataset):

    insert_query = f"""
    INSERT INTO `{gcp_project}.{bq_dataset}.{target}`
    SELECT * FROM `{gcp_project}.{bq_dataset}.{source}`
    """
    bq_client.query(insert_query)
    logging.info(f"Tempory table {source} insered in {target}.")

    bq_client.delete_table(f'{gcp_project}.{bq_dataset}.{source}')
    logging.info(f"Tempory table {source} deleted.")

def get_last_update_bq(bq_client, tables_names, gcp_project, bq_dataset):
    try:
        query = query_date_constructor(tables_names, gcp_project, bq_dataset)
        query_job = bq_client.query(query)
        
        for row in query_job.result():
            last_update = row.last_update

        if last_update:
            if isinstance(last_update, str):
                # Si c'est une chaîne, convertir en objet datetime
                last_update = datetime.strptime(last_update, '%Y-%m-%dT%H:%M:%S.%f%z')

            noumea_tz = pytz.timezone("Pacific/Noumea")
            last_update_noumea = last_update.astimezone(noumea_tz)

            # Convertir au format Jira
            jira_format_date = last_update_noumea.strftime('%Y/%m/%d %H:%M')
            logging.debug(f"Formated date for Jira: {jira_format_date}")
        else:
            logging.warning("No date gathered from BigQuery.")
            return None

        return jira_format_date
    
    except Exception as e:
        logging.error(f"An error occured : {e}")
        
        return None

def string_to_list(string_data):
    # format "'data1','data2','data3'"
    if not string_data.strip():
        return []
    else:
        return [item.strip().strip("'") for item in string_data.split(',')]
    
def query_date_constructor(tables_names, gcp_project, bq_dataset):
    query_from = f"SELECT Mise_a_jour \n      FROM `{gcp_project}.{bq_dataset}.{tables_names[0]}`"
    for table_name in tables_names[1:]:
        query_from = f"{query_from} \n      UNION ALL SELECT Mise_a_jour FROM `{gcp_project}.{bq_dataset}.{table_name}`"

    query =f"""
    SELECT MAX(Mise_a_jour) last_update
    FROM (
      {query_from}
      )
    """
    return query

def rm_updated_jira(bq_client, tables_names, source, gcp_project, bq_dataset):
    delete_query = ""

    for table_name in tables_names:
        delete_query += f"""
DELETE FROM `{gcp_project}.{bq_dataset}.{table_name}`
WHERE jira IN (SELECT jira FROM `{gcp_project}.{bq_dataset}.{source}`);
        """

    query_job = bq_client.query(delete_query)
    query_job.result()
    bq_client.delete_table(f'{gcp_project}.{bq_dataset}.{source}')
    logging.info(f"Tempory table {source} deleted.")

#----------------------------
# Fonction principale
#----------------------------

def jira_to_bq(jira_project, gcp_project, bq_dataset, jira_token, exclusion_fields="", exclusion_types=""):

    # Clients
    bq_client = bigquery.Client(credentials=scoped_credentials)
    jira = JIRA(server=JIRA_URL, token_auth=jira_token)

    field_mapping = get_fields_map(jira)
    pj_issues_types = jira.issue_types_for_project(projectIdOrKey=jira_project)

    exclusion_fields = string_to_list(exclusion_fields)
    exclusion_types = string_to_list(exclusion_types)

    pj_issues_types_names = [issue_type.name for issue_type in pj_issues_types]
    pj_issues_types_tables_names = [f"{to_bigquery_name(issue_type.name)}{'_subtask' if issue_type.subtask else ''}"  for issue_type in pj_issues_types]
    
    jira_format_date = get_last_update_bq(bq_client, pj_issues_types_tables_names, gcp_project, bq_dataset)

    jql_query = f'project={jira_project}'

    if jira_format_date:
        jql_query = jql_query +  f' AND updated > "{jira_format_date}"'

    try:
        issues = fetch_all_issues(jira, jql_query)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des issues: {e}")
        

    issues = traverse_and_convert(issues, jira_api_date_pattern)

    changed_issues = None

    if jira_format_date:
        changed_issues = [{'jira':issue['jira']} for issue in issues]
        
    if jira_format_date:
        load_to_bq(bq_client, changed_issues, '_keys_to_rm_', gcp_project, bq_dataset)
        rm_updated_jira(bq_client, pj_issues_types_tables_names, '_keys_to_rm_', gcp_project, bq_dataset)
    
    for issue_name, issue_table_name_bq in zip(pj_issues_types_names, pj_issues_types_tables_names):

        if issue_name not in exclusion_types:

            logging.info("Treatment of  %s", issue_name)
            issue_type_list = [issue for issue in issues if issue['issuetype']['name'] == issue_name]

            if len(issue_type_list) != 0:
                if jira_format_date:
                    initial_table = issue_table_name_bq
                    issue_table_name_bq = '_tmp_' + issue_table_name_bq
                    # schema = get_bigquery_schema(bq_client, initial_table, gcp_project, bq_dataset)

                issue_type_list = format_issues(issue_type_list, field_mapping, exclusion_fields)
                issue_type_list = clean_empty_structs(issue_type_list)
                load_to_bq(bq_client, issue_type_list, issue_table_name_bq, gcp_project, bq_dataset)

                if jira_format_date:
                    merge_tables(bq_client, initial_table, issue_table_name_bq, gcp_project, bq_dataset)
            logging.info("End of treatment of  %s", issue_name)
    
    return "ok"

if __name__ == "__main__":
    args = parse_arguments()

    jira_to_bq(args.jira_project, args.gcp_project, args.bq_dataset, args.jira_token, args.exclusion_fields, args.exclusion_types)
