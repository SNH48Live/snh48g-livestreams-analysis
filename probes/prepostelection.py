#!/usr/bin/env python3

import csv
import pathlib
import sys

import arrow


HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
DATA_DIR = ROOT / 'data'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
PROCESSED_INDIVIDUAL_DATA_DIR = PROCESSED_DATA_DIR / 'members'
PROBE_DATA_DIR = HERE / 'prepostelection'


sys.path.insert(0, ROOT.as_posix())
from e5 import G1, G2, G3, G4


MIDTERM_TIMESTAMP = arrow.get('2018-07-08T22:00:00+08:00').timestamp * 1000
CLOSURE_TIMESTAMP = arrow.get('2018-07-28T12:00:00+08:00').timestamp * 1000
CUTOFF_TIMESTAMP = arrow.get('2018-08-09T00:00:00+08:00').timestamp * 1000


def extract_data(path):
    with path.open() as fp:
        reader = csv.DictReader(fp)
        return [(
            # Natural date
            row['date'],
            # Start timestamp (epoch milliseconds)
            int(row['start_timestamp']),
            # Duration in minutes
            round((int(row['last_seen_timestamp']) - int(row['start_timestamp'])) / 60000),
        ) for row in reader]


def format_dates(dates):
    return ' '.join('%d-%d' % (arrow.get(date).month, arrow.get(date).day) for date in sorted(dates))


def pm(val, error):
    return f'{val}\u00B1{error}'  # U+00B1 PLUS-MINUS SIGN


def dump_csv(path, rows):
    print(f'Dumping to {path}')
    with path.open('w') as fp:
        writer = csv.writer(fp)
        for row in rows:
            writer.writerow(row)


def main():
    for member_specs, filename in [(G1, 'g1.csv'), (G2, 'g2.csv'), (G3, 'g3.csv'), (G4, 'g4.csv')]:
        rows = [('成员', '总选前直播日期', '总时间（分）', '总选后直播日期', '总时间（分）', '前后时间比例')]
        for name, member_id in member_specs:
            source_path = PROCESSED_INDIVIDUAL_DATA_DIR.joinpath(f'{member_id}.csv')
            pre_dates = set()
            pre_count = 0
            pre_duration = 0
            post_dates = set()
            post_count = 0
            post_duration = 0
            for date, start_timestamp, duration in extract_data(source_path):
                if MIDTERM_TIMESTAMP <= start_timestamp < CLOSURE_TIMESTAMP:
                    pre_dates.add(date)
                    pre_count += 1
                    pre_duration += duration
                elif CLOSURE_TIMESTAMP <= start_timestamp < CUTOFF_TIMESTAMP:
                    post_dates.add(date)
                    post_count += 1
                    post_duration += duration
            if post_duration > 0:
                ratio = '\u2248%.2f' % (pre_duration / post_duration)  # U+2248 ALMOST EQUAL TO
            elif pre_duration > 0:
                ratio = '总选后不直播'
            elif name == '黄婷婷':
                ratio = '亭亭净植'
            else:
                ratio = '查无此人'
            rows.append([name,
                         format_dates(pre_dates), pm(pre_duration, pre_count * 10),
                         format_dates(post_dates), pm(post_duration, post_count * 10),
                         ratio])
        dump_csv(PROBE_DATA_DIR.joinpath(filename), rows)


if __name__ == '__main__':
    main()
