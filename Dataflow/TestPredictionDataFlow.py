import pandas as pd


from sklearn.linear_model import LinearRegression

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

input_filename = 'gs://testing-gcp-mandar/Salary_Data_Time_New.csv'
output_filename = 'gs://testing-gcp-mandar/outputPred.csv'

dataflow_options = ['--project=lofty-shine-248403', '--job_name=newjob', '--temp_location=gs://testing-gcp-mandar/temp']
dataflow_options.append('--staging_location=gs://testing-gcp-mandar/staging')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# Dataflow runner
options.view_as(StandardOptions).runner = 'dataflow'
options.view_as(SetupOptions).save_main_session = True


class Split(beam.DoFn):

    def process(self, element):
        from sklearn.model_selection import train_test_split
        import pandas as pd
        import pickle as pk
        import tensorflow as tf

        """
        Splits each row on commas and returns a dictionary representing the
        row
        """
        YearsExperience, Salary, Time = element.split(",")
        try:
            YearsExperience = int(YearsExperience)
            Salary=int(Salary)
            # Time=int(Time)
        except Exception:
            YearsExperience = 0
            Salary = 0
            # Time = 0

        data=[[YearsExperience,YearsExperience],
            [Salary, Salary]           ]
        # print (data)
        df = pd.DataFrame(data,columns=['YearsExperience', 'Salary'])
        # print(df)
        x = df[['YearsExperience']].values
        y = df[['Salary']].values
        # z=df[['Time']].values

        x_trainY, x_testY, y_trainS, y_testS = train_test_split(x, y, test_size=0.2, random_state=0)
        regresor = LinearRegression()
        regresor.fit(x_trainY, y_trainS)


        pm=open('abc.pk','wb')
        pk.dump(regresor,pm)

        filename = "gs://testing-gcp-mandar/model/google_salary.pkl"
        with tf.io.gfile.GFile(filename, 'wb') as f:
            pk.dump(regresor, f)

        pred = regresor.predict(x_testY)

        # df1 = pd.DataFrame(data, columns=['YearsExperience', 'Salary'])
        # print(df1)
        # x1 = df1[['YearsExperience']].values
        # y1= df1[['Salary']].values

        # x_train, x_test, y_train, y_test = train_test_split(x1, y1, test_size=0.2, random_state=0)
        # regresor = LinearRegression()
        # regresor.fit(x_train, y_train)
        # predT = regresor.predict(x_test)

        return [{
            'YearsExperience': x_testY,
            'Salary': pred,
            'Time': pred
        }]

class CollectSalary(beam.DoFn):
    def process(self, element):
        """
        Returns a list of tuples containing country and duration
        """
        result = [
            (element['YearsExperience'], element['Salary'])
        ]
        return result

class WriteToCSV(beam.DoFn):
    def process(self, element):
        """
        Prepares each row to be written in the csv

        """
        print (element[1]['Salary'][0])

        result = [
            "{},{},{}".format(
                element[0],
                element[1]['Time'][0],
                element[1]['Salary'][0]
            )
        ]
        return result

class CollectTime(beam.DoFn):
    def process(self, element):
        """
        Returns a list of tuples containing country and user name
        """
        result = [
            (element['YearsExperience'], element['Time'])
        ]
        return result



with beam.Pipeline(options=options) as p:
    rows = (
        p |
        beam.io.ReadFromText(input_filename) |
        beam.ParDo(Split())
    )

    salary = (
        rows |
        beam.ParDo(CollectSalary()) |
        "Grouping salary" >> beam.GroupByKey()
    )
    time = (
            rows |
            beam.ParDo(CollectTime()) |
            "Grouping timing" >> beam.GroupByKey()
    )
    to_be_joined = (
            {
                'Time': salary,
                'Salary': time
            } |
            beam.CoGroupByKey() |
            beam.ParDo(WriteToCSV()) |
            beam.io.WriteToText(output_filename)
    )