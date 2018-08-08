#!/usr/bin/env python3

# Process raw data.

import collections
import csv
import json
import pathlib
import sys
import traceback

import arrow
import attr


ROOT = pathlib.Path(__file__).resolve().parent
DATA_DIR = ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
PROCESSED_INDIVIDUAL_DATA_DIR = PROCESSED_DATA_DIR / 'members'


FIELD_NAMES = [
    'livestream_id',
    'member_id',
    'member_name',
    'date',
    'start_timestamp',
    'fist_seen_timestamp',
    'last_seen_timestamp',
]
Livestream = attr.make_class('Livestream', FIELD_NAMES)


def dump_livestreams_csv(path, livestreams):
    print(f'Dumping to {path}')
    with path.open('w') as fp:
        writer = csv.writer(fp)
        writer.writerow(FIELD_NAMES)
        for livestream in livestreams:
            fields = [getattr(livestream, fn) for fn in FIELD_NAMES]
            writer.writerow(fields)


def main():
    livestream_store = dict()  # Indexed by livestream_id
    processed_dirs = set()
    for p in sorted(RAW_DATA_DIR.glob('*/*.json')):
        subdir = p.parent
        if subdir not in processed_dirs:
            print(f'Processing {subdir}', file=sys.stderr)
            processed_dirs.add(subdir)

        if p.stat().st_size == 0:
            continue
        with p.open() as fp:
            timestamp = int(p.stem) * 1000
            try:
                for entry in json.load(fp) or []:
                    livestream_id = entry['liveId']
                    member_id = entry['memberId']
                    title = entry['title']
                    member_name = title.split('的')[0]
                    assert member_name
                    start_timestamp = entry['startTime']
                    start_time = arrow.get(start_timestamp / 1000).to('+08:00')
                    if start_time.hour < 5:
                        # If a livestream started before 5am, categorize it in the previous day.
                        date = start_time.shift(days=-1).date()
                    else:
                        date = start_time.date()
                    livestream = livestream_store.setdefault(livestream_id, Livestream(
                        livestream_id=livestream_id,
                        member_id=member_id,
                        member_name=member_name,
                        date=date,
                        start_timestamp=start_timestamp,
                        fist_seen_timestamp=timestamp,
                        last_seen_timestamp=timestamp,
                    ))
            except Exception:
                traceback.print_exc()
                sys.exit(f'Error processing {p}')

    livestreams = sorted(livestream_store.values(), key=lambda l: l.start_timestamp)
    dump_livestreams_csv(PROCESSED_DATA_DIR.joinpath('master.csv'), livestreams)

    livestreams_by_member = collections.defaultdict(list)
    for livestream in livestreams:
        livestreams_by_member[livestream.member_id].append(livestream)
    for member_id in sorted(livestreams_by_member.keys()):
        dump_livestreams_csv(PROCESSED_INDIVIDUAL_DATA_DIR.joinpath(f'{member_id}.csv'),
                             livestreams_by_member[member_id])


if __name__ == '__main__':
    main()
