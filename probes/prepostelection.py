#!/usr/bin/env python3

# Intro & methodology:
#
# 凌晨承诺白天会做一个呼声较高的总选前后对比，这就来了。
#
# 首先要选取一个“总选前”的开始时间点。这个点不能太早，另外部分人表示自家
# 7月直播主要在中报前（表示怀疑），所以选取了中报作为开始时间点，具体是7
# 月8日22时。转折点很简单，五选投票通道关闭，7月28日12时。结束时间就是发
# 贴前的8月9日0时。总结一下，7月8日22时至7月28日12时的直播算在“总选前”直
# 播中，7月28日12时至8月9日0时的直播算在“总选后”直播中。
#
# 直播日期依然是按凌晨5点为界，5点前算在前一天里（5点的优势在于早睡的基
# 本没起，晚睡的基本睡了，是很自然的日界线）。
#
# 受本人获取数据方式所限，每次直播结束时间有±10分钟的误差，这个误差我原
# 封不动显示在了下面的表格中。其实未删的直播是可以查到精确时长的，但我不
# 想多花时间了，毕竟就是看个大概罢了。
#
# 不得不说，这样看是有点刺激的。

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
