# -*- coding: utf-8 -*-
import datetime

import haversine
import pandas as pd
from fuzzywuzzy import process
from pandas import Timestamp, Timedelta

from Backend.EVENT_parser.SFR_parse import SFR_parsing
from Backend.EVENT_parser.SI_parse import SI_parsing
from Backend.EVENT_parser.common_functions import web_parse
from Backend.EVENT_parser.geotable import geotable
from Backend.EVENT_parser.split import SPL


def EVENT_DATA(link):
    page = web_parse(link)
    mode = 1 if 'WinOrient' in str(page.find('title')) else 0
    if mode == 1:
        return SI_parsing(page)
    elif mode == 0:
        return SFR_parsing(page)


def find_most_similar_index(input_string, dataframe):
    index_choices = dataframe.index.tolist()
    most_similar_index, similarity_score = process.extractOne(input_string, index_choices)
    if most_similar_index == 0:
        return 0, 0
    else:
        name = most_similar_index.split('^')[0]
        group = most_similar_index.split('^')[1]
        return name, group


def stops_3sec_more(dft):
    con = 0
    general = 0
    for pace in dft['spd']:
        if pace < 4:
            con += 1
        else:
            con = 0
        if con >= 3:
            general += 1
            # con = 0
    return general


def calculate_metrics(dfs, dft):
    start = dft.index.to_list()[0]
    dfs['n'] = dfs.index
    dfs.index = dfs['gt'].astype('timedelta64[ns]').apply(lambda x: start + x)
    control_points = get_control_points(dfs)
    dfa = {}
    coord = {}
    hav = lambda x, y: haversine.haversine(x, y) * 1e3
    for v, k in control_points.items():
        start_p = dft.loc[k[0], ['lat', 'lon']].values
        fin_p = dft.loc[k[1], ['lat', 'lon']].values
        d2d_straight = hav(start_p, fin_p)  # meters
        d2d_path = dft.loc[k[0]:k[1], 'dist_2d'].sum()
        alt_dif = dft.loc[k[1], 'alt'] - dft.loc[k[0], 'alt']
        climb = dft[dft['alt_diff'] > 0].loc[k[0]:k[1], 'alt_diff'].sum()
        down = dft[dft['alt_diff'] < 0].loc[k[0]:k[1], 'alt_diff'].sum()
        path_coef = d2d_path / d2d_straight if d2d_straight != 0 else 2
        spd_eff = (d2d_straight / (Timestamp(k[1]) - Timestamp(k[0])).total_seconds()) * 3.6  # km/h
        spd_real = (d2d_path / (Timestamp(k[1]) - Timestamp(k[0])).total_seconds()) * 3.6  # km/h
        spd_std = dft.loc[k[0]:k[1], 'spd'].std()
        spd_max = dft.loc[k[0]:k[1], 'spd'].max()
        spd_min = dft.loc[k[0]:k[1], 'spd'].min()
        pace = dft.loc[k[0]:k[1], 'pace'].median()
        stops = stops_3sec_more(dft.loc[k[0]:k[1], :])

        d = {
            'xy': d2d_straight,
            'path': d2d_path,
            'dif': path_coef,
            'a_dif': alt_dif,
            'climb': climb,
            'down': down,
            'spde': spd_eff,
            'spdr': spd_real,
            'spd_std': spd_std,
            'spd_max': spd_max,
            'spd_min': spd_min,
            'stops': stops,
            'pace': pace
        }
        dfa.setdefault(v, d)
        stx = dft.loc[k[0], 'lat']
        sty = dft.loc[k[0], 'lon']
        fnx = dft.loc[k[1], 'lat']
        fny = dft.loc[k[1], 'lon']
        coord.setdefault(v, {'stx': stx, 'sty': sty, 'fnx': fnx, 'fny': fny})
    pd.DataFrame(coord).to_csv('coord_data.csv')
    dft.to_csv('track_data.csv')
    dfs.set_index('n', inplace=True)
    dfa = pd.DataFrame(dfa).T
    df_full = pd.concat([dfs, dfa], join='outer', axis=1)

    return df_full


def get_control_points(dfs):
    control_points = {}
    for i in dfs.index:
        control_points.setdefault(dfs.loc[i, 'n'], (str(i - dfs.loc[i, 's']), str(i - Timedelta(seconds=1))))
    return control_points


def GEOSPL(link, name, gpxpath='', start=''):
    print(2, datetime.datetime.now())
    df, routes, log = EVENT_DATA(link)
    athlet_name, group = find_most_similar_index(name, df)
    if athlet_name == 0:
        return None, None, None, None
    print(3, datetime.datetime.now())
    dfs, res_df = SPL(df, routes, athlet_name, group)
    full_index = f'{athlet_name}^{group}'
    res = df.loc[full_index, 'RES']
    mode = 0 if gpxpath == '' else 1
    if gpxpath == '':
        return dfs, res_df, full_index, mode
    try:
        dft = geotable(gpxpath, result=res, start_time=start)
        df = calculate_metrics(dfs, dft)
        return df, res_df, full_index, mode
    except:
        mode = 0
        return dfs, res_df, full_index, mode


def create_post(name: str, surname: str, link: str, gpxfile: str = "", start: str = "") -> dict:
    fullname = f'{name.upper()} {surname.upper()}'
    print(1, datetime.datetime.now())
    df, res_df, index, success_gpx = GEOSPL(link, fullname, gpxpath=gpxfile, start=start)
    df: pd.DataFrame = df
    print(4, datetime.datetime.now())
    backlog: pd.Timedelta = res_df.loc[index, 'l_bk']
    vc = df['s_p'].value_counts()
    split_firsts = 0 if 1 not in vc else vc[1]
    place = res_df.sort_values(by='res', ascending=True).index.tolist().index(index) + 1
    p_bk_median = df[df['p_bk'] != '-']['p_bk'].median()
    bk_median: pd.Timedelta = df[df['bk'] != '-']['bk'].apply(lambda x: x.total_seconds()).median()
    points_number = df.shape[0]

    d = {
        'event_id_pk': 0,
        'user_id_pk': 0,
        'post_name': 0,
        'backlog': backlog.total_seconds(),
        'split_firsts': int(split_firsts),
        'place': place,
        'median(p_bk)': float(p_bk_median),
        'median(bk)': float(bk_median),
        'image_path': 0,
        'success_gpx': success_gpx,
        'points_number': points_number,
        'split': df.to_dict()
    }
    # return {'response': d}

    if success_gpx == 1:
        lenght_s = df['xy'].sum()
        lenght_p = df['path'].sum()
        climb = df[df['climb'] >= 0]['climb'].sum()
        pace = df['pace'].median()
        d1 = {
            'location': 0,
            'gpx_path': gpxfile,
            'lenght(s)': float(lenght_s),
            'lenght(p)': float(lenght_p),
            'climb': float(climb),
            'pace': float(pace),
            'split': df.to_dict()
        }
        [print(type(i)) for i in d1.values()]
        for k in d1:
            d.setdefault(k, d1[k])
    return d

# df, r, l = EVENT_DATA(
#     'http://orientpskov.ru/uploads/files/meropriyatia/%D0%92%D0%A1%D0%90%D0%9D2023%D0%A1%D0%9F%D0%9B%D0%98%D0%A22%D0%94%D0%95%D0%9D%D0%AC.htm')
#
# dfs, res_df = SPL(df,r,'ИВАНОВ ПАВЕЛ', "МЭ")
# dfs.to_csv('1.csv')
# s = datetime.datetime.now()
# d = create_post('Павел',"Иванов","https://fsono.ru/wp-content/uploads/2022/03/20220329_spl.htm")
# print(datetime.datetime.now() - s, d)
