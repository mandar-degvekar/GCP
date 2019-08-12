import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline('Directrunner')

lines = \
(p | "reagfhfhfhgfdfromfile" >> beam.io.ReadFromText('a.txt')
| "firsttreamfoirm" >> beam.FlatMap(lambda word: [len(word)])
| "outpugdfgdtfile" >> beam.io.WriteToText('MBA2.mp4')
)
p.run()
