import apache_beam as beam


# Define a simple function to transform the data
def format_result(word_count):
    (word, count) = word_count
    return f'{word}: {count}'


# Define the pipeline
with beam.Pipeline() as pipeline:
    # Read from a text file.
    counts = (
            pipeline
            | 'Read' >> beam.io.ReadFromText('input.txt')
            | 'Split' >> beam.FlatMap(lambda x: x.split())
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Apply a transformation to format the results
    formatted_counts = counts | 'Format' >> beam.Map(format_result)

    # Write to an output text file
    formatted_counts | 'Write' >> beam.io.WriteToText('output')

# Run the pipeline
if __name__ == '__main__':
    pipeline.run()
