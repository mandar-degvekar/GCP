import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

dataflow_options = ['--project=lofty-shine-248403', '--job_name=newjob', '--temp_location=gs://testing-gcp-mandar/temp']
dataflow_options.append('--staging_location=gs://testing-gcp-mandar/staging')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# Dataflow runner
options.view_as(StandardOptions).runner = 'DirectRunner'
options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).streaming = True

table_schema = {'fields': [
    {'name': 'Predicted_Value', 'type': 'FLOAT', 'mode': 'REQUIRED'},
    {'name': 'Open', 'type': 'FLOAT'}
]}

def MLmodel(data):
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split
    import pandas as pd

    data_tosendto_bg = []
    ad = eval(data)
    for item in val['Time Series (1min)'].items():
        d = item[1]
        d['date'] = item[0]
        data_tosendto_bg.append(d)

    df = pd.DataFrame(data_tosendto_bg)
    df.columns = ['Open','High','Low', 'Close', 'Volume','Date']
    # setting index as date
    df.index = df['Date']

    train_data=pd.DataFrame(index=range(0, len(df)), columns=['Open','Close'])


    train_data=train_data.iloc[1:,:]
    x = df[['Open']].values
    y = df[['Close']].values
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)
    regresor = LinearRegression()
    regresor.fit(x_train, y_train)




    preds = regresor.predict(x_test)
    l1 =[]
    i = 0
    for val in preds:
        temp = {}
        temp['Open']=float(str(x[i]).replace('u','').replace('[','').replace(']','').replace('\'',''))
        temp['Predicted_Value'] = float(str(preds[i]).replace('[','').replace(']',''))
        l1.append(temp)
        i = i + 1
    print(l1)
    return l1

p = beam.Pipeline(options=options)

lines = \
    (p | 'Read Data From PubSub' >> beam.io.ReadFromPubSub(subscription='projects/lofty-shine-248403/subscriptions/SubPred').with_output_types(bytes)
    # | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
    | 'Build Model' >> beam.ParDo(MLmodel)
    |beam.io.WriteToBigQuery(
            schema=table_schema,
            table="lofty-shine-248403:my_new_datasset.StockPredPubSub")
    )




p.run().wait_until_finish()