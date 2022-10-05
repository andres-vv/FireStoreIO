import json
import apache_beam as beam

class Transform(beam.DoFn):
    def process(self, element, *args, **kwargs):
        element = json.loads(element)
        element['length'] = len(element['text'])
        yield str(json.dumps(element))
