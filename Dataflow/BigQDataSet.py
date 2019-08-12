from google.cloud import bigquery
from google.cloud import storage

def bq_load_csv_in_gcs():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_new_datasset')

    job_config = bigquery.LoadJobConfig()
    schema = [
        bigquery.SchemaField('Years', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('salary', 'STRING', mode='REQUIRED'),
    ]
    job_config.schema = schema
    job_config.skip_leading_rows = 1

    load_job = bigquery_client.load_table_from_uri('gs://testing-gcp-mandar/Salary_Data.csv',
                                                   dataset_ref.table('Salary'), job_config=job_config)

    assert load_job.job_type == 'load'

    load_job.result()  # Waits for table load to complete.

    assert load_job.state == 'DONE'

bq_load_csv_in_gcs()



# from google.cloud import bigquery


# def bq_create_table():
#     bigquery_client = bigquery.Client()
#     dataset_ref = bigquery_client.dataset('my_new_datasset')
#
#     # Prepares a reference to the table
#     table_ref = dataset_ref.table('Salary')
#
#     try:
#         bigquery_client.get_table(table_ref)
#     except Exception:
#         schema = [
#             bigquery.SchemaField('Years', 'STRING', mode='REQUIRED'),
#             bigquery.SchemaField('salary', 'STRING', mode='REQUIRED'),
#         ]
#         table = bigquery.Table(table_ref, schema=schema)
#         table = bigquery_client.create_table(table)
#         print('table {} created.'.format(table.table_id))
#
# bq_create_table()


# from google.cloud import bigquery
# import NotFound
#
# def bq_create_dataset():
#     bigquery_client = bigquery.Client()
#     dataset_ref = bigquery_client.dataset('my_new_datasset')
#
#     try:
#         bigquery_client.get_dataset(dataset_ref)
#     except Exception:
#         dataset = bigquery.Dataset(dataset_ref)
#         dataset = bigquery_client.create_dataset(dataset)
#         print('Dataset {} created.'.format(dataset.dataset_id))
#
# bq_create_dataset()
