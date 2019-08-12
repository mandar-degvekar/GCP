from sklearn.linear_model import LinearRegression

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

dataflow_options = ['--project=lofty-shine-248403', '--job_name=newjob', '--temp_location=gs://testing-gcp-mandar/temp']
dataflow_options.append('--staging_location=gs://testing-gcp-mandar/staging')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# Dataflow runner
options.view_as(StandardOptions).runner = 'dataflow'
options.view_as(SetupOptions).save_main_session = True

table_schema = {'fields': [
    {'name': 'Years', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'salary', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'time', 'type': 'STRING', 'mode': 'REQUIRED'}
]}

input_filename = 'gs://testing-gcp-mandar/Salary_Data_Time_New.csv'
output_filename = 'gs://testing-gcp-mandar/outputPredPickle.csv'

class Split(beam.DoFn):



    def process(self,element):
        import pandas as pd
        from google.cloud import storage
        import pickle as pk

        YearsExperience, Salary, Time = element.split(",")
        try:
            YearsExperience = int(YearsExperience)
            Salary = int(Salary)
            # Time=int(Time)
        except Exception:
            YearsExperience = 0
            Salary = 0
            # Time = 0

        data = [[YearsExperience, YearsExperience],
                [Salary, Salary]]
        # print (data)
        df = pd.DataFrame(data, columns=['YearsExperience', 'Salary'])
        # print(df)
        x = df[['YearsExperience']].values

        storage_client = storage.Client()
        bucket = storage_client.get_bucket("testing-gcp-mandar")
        blob = bucket.blob("model/google_salary.pkl")
        model_local = "google_salary.pkl"
        blob.download_to_filename(model_local)

        pkk = pk.load(open(model_local,'rb'))
        pred = pkk.predict(x)



        l=[{

            'Years': ''.join(e for e in str(x[0]) if e.isalnum()),
            'salary':''.join(e for e in str(pred[0]) if e.isalnum()),
            'time': ''.join(e for e in str(pred[0]) if e.isalnum())
        }]

        print (l)
        return l


with beam.Pipeline(options=options) as p:
    rows = (
            p |
            beam.io.ReadFromText(input_filename) |
            beam.ParDo(Split()) |
            beam.io.WriteToBigQuery(
            schema=table_schema,
            table="lofty-shine-248403:my_new_datasset.testSal")

        )
