from google.cloud import bigquery
def bq_create_table():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_new_datasset')

    # Prepares a reference to the table
    table_ref = dataset_ref.table('testSal')

    try:
        bigquery_client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField('Years', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('salary', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('time', 'STRING', mode='REQUIRED')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))

bq_create_table()
