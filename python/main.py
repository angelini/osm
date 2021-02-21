#!/usr/bin/env python

import argparse
import pathlib as pl

import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.parquet as pq

PROJECT_ROOT = pl.Path(__file__).parent.absolute().parent
DATA_DIR = PROJECT_ROOT / 'data'


def convert_csv(source: pl.Path, target: pl.Path):
    if not source.exists():
        raise RuntimeError('source does not exist')

    if target.exists():
        if target.is_dir():
            raise RuntimeError('target exists and is a directory')
        target.unlink()

    table = pc.read_csv(source)
    print(f'[convert_csv] read {table.num_rows} rows from {source}')

    pq.write_table(table, target, version='2.0', compression='BROTLI')
    print(f'[convert_csv] wrote to {target}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sources', nargs='*', help='csv file names to convert')
    args = parser.parse_args()

    for source in args.sources:
        convert_csv(
            DATA_DIR / 'csv' / source,
            (DATA_DIR / 'parquet' / source).with_suffix('.parquet')
        )
