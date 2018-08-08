#!/usr/bin/env python3

# Intro & methodology:
#
# 自从几个月前大号被百度永封了就没怎么来hhy，来了也不发贴回贴。今天听说
# 有人统计了五选周期每人的直播次数，就来看了一下，发现受统计方法所限，原
# 贴数据问题很大（例：路人最爱的那位大人分明直播了两次——虽然都是外务要求；
# 但原贴说那位大人直播了零次，以致收了很多酸菜，本人心痛不已），所以本人
# 决定小号出山，拿出箱底的数据拨乱反正一下，也顺便提供一点细节。
#
# 还是老样子，本人先介绍数据来源、统计方法，指出局限性；然后提供数据，但
# 不多加主观发挥，由读者自行理解。
#
# 本人数据来源是每整十分钟爬一次口袋的当前直播列表，所以很好地解决了删直
# 播问题（不过全程都在两个整十分钟之间的直播就无法统计到，但这种直播应该
# 很少，且过于敷衍，不算也罢）。并且，关联口袋的一直播也进入了统计（未关
# 联口袋的一直播无法统计）。遗憾的是，本人从2017年11月13日起爬，所以无法
# 提供整个五选周期的数据，所以本贴将提供2017年12月至2018年7月每月的数据。
# 表中所有数据都将是【直播的天数而非次数】，所以一天开多次（一般是技术问
# 题所致）只记一次；另外，【凌晨5点前开的直播均算在前一日】，所以零点后
# 卡了重开不会算两次。所有原始数据、处理后的数据、处理用的代码均发布在
# git.io/snh48g-la ，可自行验证。
#
# 该说的都说完了，下面奉上五选圈内成员数据，每组一个表。赶时间，表比较丑，
# 凑合着看。顺便说一句，目前本人已记录了近两万次直播数据（并公开——见上一
# 段）。

import collections
import csv
import pathlib


HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
DATA_DIR = ROOT / 'data'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
PROCESSED_INDIVIDUAL_DATA_DIR = PROCESSED_DATA_DIR / 'members'
PROBE_DATA_DIR = HERE / 'monthly'


MONTHS = [
    '2017-12',
    '2018-01',
    '2018-02',
    '2018-03',
    '2018-04',
    '2018-05',
    '2018-06',
    '2018-07',
]

G1 = [
    ('李艺彤', 16),
    ('黄婷婷', 22),
    ('冯薪朵', 7),
    ('陆婷', 34),
    ('莫寒', 35),
    ('赵粤', 27),
    ('许佳琪', 21),
    ('戴萌', 38),
    ('钱蓓婷', 36),
    ('林思意', 24),
    ('谢蕾蕾', 63572),
    ('吴哲晗', 39),
    ('孔肖吟', 19),
    ('苏杉杉', 327597),
    ('段艺璇', 63554),
    ('张语格', 1),
]

G2 = [
    ('孙芮', 8),
    ('郑丹妮', 327575),
    ('宋昕冉', 6738),
    ('张丹三', 6747),
    ('刘力菲', 327567),
    ('徐子轩', 14),
    ('杨冰怡', 6744),
    ('韩家乐', 459999),
    ('易嘉爱', 33),
    ('万丽娜', 25),
    ('张雨鑫', 5574),
    ('姜杉', 63560),
    ('冯思佳', 327587),
    ('刘增艳', 63566),
    ('张怡', 63582),
    ('费沁源', 63555),
]

G3 = [
    ('张怀瑾', 407127),
    ('陈珂', 63548),
    ('马玉灵', 327596),
    ('唐莉佳', 327571),
    ('陈倩楠', 327601),
    ('黄恩茹', 407106),
    ('胡晓慧', 63559),
    ('李宇琪', 20),
    ('李钊', 6735),
    ('左婧媛', 327577),
    ('陈美君', 63549),
    ('洪珮雲', 63558),
    ('徐诗琪', 399674),
    ('赵佳蕊', 460005),
    ('青钰雯', 327581),
    ('王诗蒙', 459989),
]

G4 = [
    ('沈梦瑶', 49005),
    ('肖文铃', 327573),
    ('卢静', 327569),
    ('许杨玉琢', 5566),
    ('王雨煊', 407168),
    ('谢妮', 45),
    ('刘倩倩', 327568),
    ('孙珍妮', 286977),
    ('李梓', 327591),
    ('高源婧', 63557),
    ('刘姝贤', 327579),
    ('葛司琪', 407104),
    ('张琼予', 327560),
    ('蒋芸', 17),
    ('袁雨桢', 5),
    ('祁静', 399672),
    ('闫明筠', 6745),
    ('胡丽芝', 528094),
]


def extract_dates(path):
    with path.open() as fp:
        reader = csv.DictReader(fp)
        return [row['date'] for row in reader]


def dump_frequency_csv(path, rows):
    print(f'Dumping to {path}')
    with path.open('w') as fp:
        writer = csv.writer(fp)
        title_row = ['成员'] + MONTHS + ['总天数']
        writer.writerow(title_row)
        for row in rows:
            writer.writerow(row)


def main():
    for member_specs, filename in [(G1, 'g1.csv'), (G2, 'g2.csv'), (G3, 'g3.csv'), (G4, 'g4.csv')]:
        rows = []
        for name, member_id in member_specs:
            dates = extract_dates(PROCESSED_INDIVIDUAL_DATA_DIR.joinpath(f'{member_id}.csv'))
            monthly = collections.Counter(d[:7] for d in set(dates))
            fields = [name]
            total = 0
            for month in MONTHS:
                count = monthly.get(month, 0)
                total += count
                fields.append(count)
            fields.append(total)
            rows.append(fields)
        dump_frequency_csv(PROBE_DATA_DIR.joinpath(filename), rows)


if __name__ == '__main__':
    main()
