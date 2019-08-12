from google.cloud import bigquery
import json
import pandas as pd
import apache_beam as beam
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import numpy as np
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions



def GetMLDone(data):
    data=data[1]
    df = pd.DataFrame(data, columns=('YearsExperience', 'Salary'))
    df=df.iloc[1:,:]
    x = df[['YearsExperience']].values
    y = df[['Salary']].values
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)
    regresor = LinearRegression()
    regresor.fit(x_train, y_train)
    pred=regresor.predict(x_test)
    # return pred


    xlist = list(x_train)
    ylist = list(pred)
    l1 = []
    i=0
    # temp=''
    for xval in xlist:
        for yval in ylist:
            temp = {}
            try:
                temp['Years'] = str(xval[0])
                temp['salary'] = str(yval[0])
                l1.append(temp)
            except Exception:
                temp['Years'] = ' '
                temp['salary'] = ' '
                l1.append(temp)

    print(type(l1))
    return l1
    # return [{'Years':'hjk', 'salary':'gvj'},{'Years':'hjk', 'salary':'gvj'}]

table_schema = {'fields': [
    {'name': 'Years', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'salary', 'type': 'STRING', 'mode': 'REQUIRED'}
]}

def printer(m):
    print(m)


p = beam.Pipeline('Directrunner')
data_from_source = (p
                    | 'ReadMyFile' >> beam.io.ReadFromText('gs://testing-gcp-mandar/Salary_Data.csv')
                    | 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split(',')))
                    | 'Map record to 1' >> beam.Map(lambda record: ('M', record))
                    | 'GroupBy the data' >> beam.GroupByKey()
                    | 'Get the prediction' >> beam.ParDo(GetMLDone)
                    | beam.io.WriteToBigQuery(
                        schema=table_schema,
                        table="lofty-shine-248403:my_new_datasset.Prediction_Salaray",
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                    )

p.run()
