import os, re, logging
import argparse

from jira import JIRA
import pandas as pd
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

#----------------------------
# auth
#----------------------------
# jira_token = ""
# GCP
CRED_FILE = '' # ex: prj-davar-p-bq-a01c-c04f4c56dc89.json
ENV_LOCAL = os.path.exists(CRED_FILE)
# Authenticate google drive and bigquery APIs using credentials.
SCOPES = [ 'https://www.googleapis.com/auth/bigquery']

if ENV_LOCAL:
    creds = service_account.Credentials.from_service_account_file(CRED_FILE)
else:
    creds, _ = google.auth.default()

# retrive credentials for scopes defined.
scoped_credentials = creds.with_scopes(SCOPES)

#----------------------------
# logging
#----------------------------

if not ENV_LOCAL:
    logging.basicConfig(level=logging.INFO)
    log_client = google.cloud.logging.Client()

    # Retrieves a Cloud Logging handler based on the environment
    # you're running in and integrates the handler with the
    # Python logging module. By default this captures all logs
    # at INFO level and higher
    log_client.setup_logging()
else:
    logging.basicConfig(level=logging.DEBUG) # NOTSET DEBUG INFO WARNING ERROR CRITICAL

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
    # Limiter la longueur du nom à 128 caractères, comme requis par BigQuery
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


def load_to_bq(client, issues, table_name, gcp_project, bq_dataset, schema): 

    destination = f"{gcp_project}.{bq_dataset}.{table_name}"


    if schema is None:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",
                                        autodetect = True
                                        )
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",
                                            schema = schema
                                            )
        
    job = client.load_table_from_json(issues, destination, job_config=job_config)

    job.result()  # Attendre la fin du job

    logging.info("Data for issue type '%s' have been insered successfully.", table_name)

def reformat_timestamp(timestamp):
    """
    Reformat a date for BigQuery (YYYY-MM-DD HH:MM:SS.ssssss).
    """
    if not timestamp:
        return None
    try:
        # Tente de parser différents formats de timestamp
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")  # Avec timezone
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")  # Format UTC
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            logging.error(f"Error : can't convert {timestamp}")
            return None

def format_issues(issues, field_mapping, schema, exclusion_fields=[]):

    issues = [{field_mapping.get(k, k): v for k, v in item.items()} for item in issues]

    if len(exclusion_fields) != 0:
        issues = [{k: v for k, v in item.items() if k not in exclusion_fields} for item in issues]

    if schema:
        issues = format_parser(issues, schema)

    return issues

def clean_empty_structs(data):
    """Clean the empty structures in a list of dict (for BQ load)."""
    if isinstance(data, list):
        return [clean_empty_structs(item) for item in data]
    elif isinstance(data, dict):
        return {k: clean_empty_structs(v) if v != {} else None for k, v in data.items()}
    else:
        return data

# def create_df(issues, field_mapping):
#         data = []
#         for issue in issues:
#             row = {}
#             row['jira'] = issue.key
#             for key, value in issue.fields.__dict__.items():
#                 if key not in exclusion_fields:
#                     if key != 'parent':
#                         row_key = field_mapping[key]
#                     else:
#                         row_key = key
#                     row_value = cast_value(value)

#                     row[row_key] = row_value

#             data.append(row)
                
#         return pd.DataFrame(data)

def merge_tables(bq_client, target, source, gcp_project, bq_dataset, union_table):
    delete_query  = f"""
    DELETE FROM `{gcp_project}.{bq_dataset}.{target}`
    WHERE jira IN (
    {union_table}
    )
    """
    bq_client.query(delete_query )
    insert_query = f"""
    INSERT INTO `{gcp_project}.{bq_dataset}.{target}`
    SELECT * FROM `{gcp_project}.{bq_dataset}.{source}`
    """
    bq_client.query(insert_query)

    bq_client.delete_table(f'{gcp_project}.{bq_dataset}.{source}')
    logging.info(f"Tempory table {source} deleted.")

def get_last_update_date(bq_client, table_name, gcp_project, bq_dataset):
    try:
        query = f"SELECT max(Mise_a_jour) last_update FROM `{gcp_project}.{bq_dataset}.{table_name}`"
        query_job = bq_client.query(query)
        
        for row in query_job.result():
            last_update = row.last_update

        if last_update:
            if isinstance(last_update, str):
                # Si c'est une chaîne, convertir en objet datetime
                last_update = datetime.strptime(last_update, '%Y-%m-%dT%H:%M:%S.%f%z')
            
            # Convertir au format Jira
            jira_format_date = last_update.strftime('%Y/%m/%d %H:%M')
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
    
def get_bigquery_schema(bq_client, table_name, gcp_project, bq_dataset):
    table_id = f"{gcp_project}.{bq_dataset}.{table_name}"
    table = bq_client.get_table(table_id)
    return table.schema

def format_parser(issues, schema_fields):
    for field in schema_fields:
        if field.field_type == 'TIMESTAMP':
            for issue in issues:
                # print(f"field : {field.name}")
                issue[field.name] = reformat_timestamp(issue[field.name])
                
        elif field.field_type == 'RECORD':
            for issue in issues:
                # Vérifier que la clé existe avant de descendre dans le RECORD
                if field.name in issue and isinstance(issue[field.name], list):
                    issue[field.name] = format_parser(issue[field.name], field.fields)

                elif field.name in issue and isinstance(issue[field.name], dict):
                    issue[field.name] = format_parser([issue[field.name]], field.fields)[0]
    return issues

def create_union_table(issues_features, gcp_project, bq_dataset):

    first_key = True

    for issue_type in issues_features.keys():
        if issues_features[issue_type]['len'] != 0:

            if first_key:
                first_key = False
                union_table = f"SELECT jira FROM `{gcp_project}.{bq_dataset}.{issues_features[issue_type]['table_name']}`"
            else:
                union_table = f"{union_table} UNION DISTINCT SELECT jira FROM `{gcp_project}.{bq_dataset}.{issues_features[issue_type]['table_name']}`"

    return union_table

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

    issues_features = dict()

    for issue_type in pj_issues_types:

        if issue_type.name not in exclusion_types:

            logging.info("Treatment of  %s", issue_type.name)
            table_name = to_bigquery_name(issue_type.name)

            if issue_type.subtask:
                table_name = table_name + '_subtask'

            jira_format_date = get_last_update_date(bq_client, table_name, gcp_project, bq_dataset)
            
            jql_query = f'project={jira_project} AND issuetype="{issue_type.name}"'
            
            schema = None
            
            # si on a une date d'update on traite le différentiel, sinon on crée une table avec toutes les issues
            if jira_format_date:
                jql_query = jql_query +  f' AND updated >= "{jira_format_date}"'
                initial_table = table_name
                table_name = '_tmp_' + table_name

                schema = get_bigquery_schema(bq_client, initial_table, gcp_project, bq_dataset)
            
            try:    
                issues = fetch_all_issues(jira, jql_query)
                
                print(len(issues))


                issues_features[issue_type] = {
                    'table_name' : table_name,
                    'jira_format_date' : jira_format_date,
                    'len' : len(issues)
                    }
                
                if jira_format_date:
                    issues_features[issue_type]['initial_table'] = initial_table
                else:
                    issues_features[issue_type]['initial_table'] = table_name


                issues = format_issues(issues, field_mapping, schema, exclusion_fields)
                issues = clean_empty_structs(issues)

                if len(issues) != 0:
                    load_to_bq(bq_client, issues, table_name, gcp_project, bq_dataset, schema)

            except Exception as e:
                logging.error(f"Erreur lors du traitement de l'issue type {issue_type.name}: {e}")
                continue

    # traitement du différentiel
    union_table = create_union_table(issues_features, gcp_project, bq_dataset)

    for issue_type in pj_issues_types:      

        if issues_features[issue_type]['jira_format_date'] is not None and issues_features[issue_type]['len'] != 0:
            merge_tables(bq_client,
                        issues_features[issue_type]['initial_table'],
                        issues_features[issue_type]['table_name'],
                        gcp_project,
                        bq_dataset,
                        union_table)


    return "ok"

if __name__ == "__main__":
    args = parse_arguments()

    jira_to_bq(args.jira_project, args.gcp_project, args.bq_dataset, args.jira_token, args.exclusion_fields, args.exclusion_types)
