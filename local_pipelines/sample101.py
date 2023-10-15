import logging

import apache_beam as beam
import click
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)


class ParseRawData(beam.DoFn):
    def process(self, lines):
        transformed_dicts = []
        logging.info(f'Lines received: {lines}')
        logging.info(f'Type of Lines received: {type(lines)}')
        lines = [lines] if not isinstance(lines, list) else lines

        for line in lines:
            parts = line.split(',') if ',' in line else None

            if not parts:
                logging.info(f'Malformed text line detected: {line}')
                continue

            _dict = {}
            dict_parts = [x.split(':') for x in parts]

            for key in dict_parts:
                _dict[key[0].strip()] = key[1].strip()

            transformed_dicts.append(_dict)

        logging.info(f'Final result: {transformed_dicts}')

        return transformed_dicts


class FormatToCSV(beam.DoFn):
    def process(self, dicts):
        csv_str_lines = []

        logging.info(f'Processing dicts: {dicts}')
        logging.info(f'Processing dicts type: {type(dicts)}')

        for line in dicts:
            print(f'Line: {line}')
            # line_as_str = ','.join(line.values())
            # csv_str_lines.append(line_as_str)

        return csv_str_lines


class ExtractColumns(beam.DoFn):
    def process(self, dicts):
        # Assumes that every dict has the same structure( number of keys
        # and its types, so we can take just first one for extracting the
        # column names of the final CSV file
        return ','.join(dicts[0].keys())


@click.command()
@click.option('--f_in', help='Input file')
@click.option('--f_out', required=False, help='Output file')
def main(f_in, f_out):
    logging.info(f'Input file extracted: {f_in}!')

    with beam.Pipeline('DirectRunner', options=PipelineOptions()) as p:
        # Create a PCollection from the input file.
        lines = p | 'ReadFromText' >> beam.io.ReadFromText(f_in)

        # Apply a ParDo transform to parse each line into valid dicts.
        dicts = lines | 'ParseRawData1' >> beam.ParDo(ParseRawData())

        csv_format = dicts | 'FormatToCSV1' >> beam.ParDo(FormatToCSV())

        # header = dicts | 'ExtractColumns1' >> beam.ParDo(ExtractColumns())

        # Write the dicts to a file.
        csv_format | 'WriteToText' >> beam.io.WriteToText(
            file_path_prefix=f_out,
            file_name_suffix='.csv',
            header='header')


if __name__ == '__main__':
    main()
