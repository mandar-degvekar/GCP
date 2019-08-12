from google.cloud import bigquery
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import numpy as np

def bq_create_table():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_new_datasset')

    # Prepares a reference to the table
    table_ref = dataset_ref.table('Prediction_Salaray')

    try:
        bigquery_client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField('Years', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('salary', 'STRING', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))

# bq_create_table()

def export_items_to_bigquery(l):
    # Instantiates a client
    bigquery_client = bigquery.Client()
    # Prepares a reference to the dataset
    dataset_ref = bigquery_client.dataset('my_new_datasset')
    table_ref = dataset_ref.table('Prediction_Salaray')
    table = bigquery_client.get_table(table_ref)  # API call
    print (table)
    errors = bigquery_client.insert_rows(table, l)  # API request
    print (errors)
    assert errors == []

file_val=pd.read_csv("Salary_Data.csv")
x_val=np.array(file_val["YearsExperience"]).reshape(len(file_val["YearsExperience"]),1)
y_val=np.array(file_val["Salary"]).reshape(len(file_val["Salary"]),1)
expval_train,expval_test,salval_train,salval_test=train_test_split(x_val,y_val,test_size=0.4)
# creating a model
reg=LinearRegression()
# training the model
reg.fit(expval_train,salval_train)
# reg.score(salval_test,reg.predict(expval_test))
pred=reg.predict(expval_test)

thisdic={}
xlist=list(expval_test)
ylist=list(pred)
list=[]

# print (xlist)
# print(ylist)

for xval in xlist:
   for yval in ylist:
       thisdic[str(xval[0])]=str(yval[0])
       list.append(thisdic)



# list = [{k, v} for k, v in thisdic.items()]
print (list)
# export_items_to_bigquery(list)










