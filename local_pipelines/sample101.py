import logging

import click

logging.basicConfig(level=logging.INFO)


def transform_raw_txt_to_dict(raw_txt):
    transformed_dicts = []

    for line in raw_txt:
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


@click.command()
@click.option('--f_in', help='Input file')
@click.option('--f_out', required=False, help='Output file')
def main(f_in, f_out):
    logging.info(f'Input file extracted, {f_in}!')

    with open(f_in) as f:
        data = f.readlines()
        logging.info(f'Data extracted: {data}')
        data = transform_raw_txt_to_dict(raw_txt=data)

    return data


if __name__ == '__main__':
    main()
