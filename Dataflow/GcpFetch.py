import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'lofty-shine-248403'
google_cloud_options.job_name = 'testingjob'
google_cloud_options.staging_location = 'gs://testing-gcp-mandar/staging'
google_cloud_options.temp_location = 'gs://testing-gcp-mandar/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

def printdata(value):
    print value




p = beam.Pipeline(options=options)

(p | "ReadFromFile" >> beam.io.ReadFromText('gs://testing-gcp-mandar/a.txt')
| "print data" >> beam.ParDo(printdata)
)
p.run()