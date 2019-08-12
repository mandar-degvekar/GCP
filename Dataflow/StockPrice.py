import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import numpy as np
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

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
    return pred


p = beam.Pipeline(options=PipelineOptions())

data_from_source = (p
                    | 'ReadMyFile' >> ReadFromText('Salary_Data.csv')
                    | 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split(',')))
                    | 'Map record to 1' >> beam.Map(lambda record: ('M', record))
                    | 'GroupBy the data' >> beam.GroupByKey()
                    | 'Get the prediction' >> beam.ParDo(GetMLDone)
                    | 'Export results to new file' >> WriteToText('output', '.txt')
                    )
p.run()