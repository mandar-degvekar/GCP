import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

input_filename = 'gs://testing-gcp-mandar/Salary_Data_Time_New.csv'
output_filename = 'gs://testing-gcp-mandar/output.csv'

dataflow_options = ['--project=lofty-shine-248403', '--job_name=newjob', '--temp_location=gs://testing-gcp-mandar/temp']
dataflow_options.append('--staging_location=gs://testing-gcp-mandar/staging')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# Dataflow runner
options.view_as(StandardOptions).runner = 'dataflow'

class Split(beam.DoFn):
    def process(self, element):
        """
        Splits each row on commas and returns a dictionary representing the
        row
        """
        YearsExperience, Salary, Time = element.split(",")
        try:
            YearsExperience = int(YearsExperience)
            Salary=int(Salary)
            Time=int(Time)
        except Exception:
            YearsExperience = 0
            Salary = 0
            Time = 0

        return [{
            'YearsExperience': YearsExperience,
            'Salary': Salary,
            'Time': Time
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
        "Grouping salary" >> beam.GroupByKey() |
        "Calculating average" >> beam.CombineValues(beam.combiners.MeanCombineFn()
        )
    )
    time = (
            rows |
            beam.ParDo(CollectTime()) |
            "Grouping timing" >> beam.GroupByKey() |
            "Counting users" >> beam.CombineValues(beam.combiners.CountCombineFn()
                                                   )
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