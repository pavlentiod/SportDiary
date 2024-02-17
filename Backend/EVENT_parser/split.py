import datetime
import re

import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from Backend.EVENT_parser.names_and_sex_js import all_names

null = pd.Timedelta(seconds=0)

not_found = []


def sort2(names, all_names):
    # print(all_names)
    male_names = []
    female_names = []
    undef = []
    for name in names:
        spl = re.split('[ ^]', name.upper())
        first_name = [i for i in spl if i in all_names]
        if len(first_name) != 0:
            if all_names[first_name[0]] == 'Ж':
                female_names.append(name)
            elif all_names[first_name[0]] == 'М':
                male_names.append(name)
        else:
            undef.append(name)
    return male_names, female_names, undef


def sortsex(names):
    sorted_names = sort2(names, all_names)
    # names_dict = {'M': sorted_names[0], 'F': sorted_names[1], 'Undefined':sorted_names[2]}
    names_dict = {key: val for key, val in zip(['M', 'F', 'Undefined'], sorted_names) if val != []}
    return names_dict


def find_sex(name):
    srt = sortsex([name])
    sex = [i for i in srt if srt[i] != []][0]
    return sex


def filter_df(df: DataFrame, routes, group='', sex=''):
    if group != '':
        group_indexes = [i for v in routes[group].values() for i in v]
        df_filter: DataFrame = df.loc[group_indexes].dropna(how='all', axis=1)
    elif sex != '':
        indexes = sortsex(df.index)[sex]
        df_filter: DataFrame = df.loc[indexes]
    else:
        df_filter = df
    return df_filter


def format(t: pd.Timedelta):
    if t in [pd.NaT, '']:
        return '-'
    if t < null:
        t = abs(null - t)
        sign = '-'
    else:
        sign = ' '
    t = re.search('\d+:\d+:\d+', str(t))
    dt = datetime.datetime.strptime(t.group(), '%H:%M:%S')
    if dt.hour == 0:
        return sign + dt.strftime('%M:%S')
    else:
        return sign + dt.strftime('%H:%M:%S')


def find_data(df, filt, name, group, routes):
    if filt == 'group':
        data = filter_df(df, routes=routes, group=group.upper())
    elif filt == 'sex':
        sex = find_sex(name)
        print(sex, name)
        if sex != 'Undefined':
            data = filter_df(df, sex=sex, routes=routes)
        else:
            data = filter_df(df, routes=routes)
    else:
        data = filter_df(df, routes=routes)
    return data


def name_format(s):
    s1 = s.split('^')
    s2 = s1[0]
    s3 = f'{s2[:(s2 + " ").index(" ") + 2]}.[{s1[-1]}]'
    return s3


def check_cp(dist: list, data: DataFrame, name: str):  # ПЕРЕПИСАТЬ, ОСТАВЛЯЕТ ПУНКТЫ КОТОРЫХ НЕТ В ФРЕЙМЕ
    if '-' in dist:
        return dist
    else:
        # print(name, name in list(data.index))
        s: Series = data.loc[name]
        # Скрестили данные фрейма и дистанции, сделали перекрест
        not_null = [i for i in s.dropna(axis=0).index]
        # Выбрали только ненулевые, заново скрестили с дистанцией
        d = [i if i in not_null else '-' for i in dist]
        # except KeyError:
        #     d = []
        #     print('keyerror')
        return d


"""
1. Делаем фрейм с группой
2. Берем рассев этого человека
3. По очереди выводим его результаты, считаем проигрыш(+Проигрыш абсолютному лидеру, если не совпадают) и место
4. Считаем общюю статистику:
Проигрыш лидеру( \%)
Прооигрыш идеальному лидеру( \%)
Оценка
"""


def event_frame(file: str):
    df: DataFrame = pd.read_csv(file, index_col=0, encoding='utf-8').apply(pd.to_timedelta)
    df.replace([pd.Timedelta(seconds=0)], pd.NA, inplace=True)
    df.dropna(how='all', axis=1)
    return df


def distance(routes: dict, name: str, group: str):
    d = routes[group]
    try:
        dist = eval([i for i in d if f'{name}^{group}' in d[i]][0])
    except:
        dist = [i for i in d if f'{name}^{group}' in d[i]][0]
    dist = [i if '0' not in str(i).split('->') else '-' for i in dist]
    return dist


def splits_list(name_data, dist, R):
    split_time = [name_data[dist[i]] if i != '-' else '-' for i in R]
    return split_time


def general_times_list(name_data, dist, nn, delim):
    part1 = dist[:nn[0]]
    part2 = dist[nn[-1] + 1:]
    try:
        general_time = [name_data[part1[:i]].sum() for i in range(1, len(part1) + 1)] + delim + [
            name_data['RES'] - name_data[part2[i:]].sum() for i in range(1, len(part2) + 1)]
    except:
        general_time = [''] * len(dist)
    return general_time


def find_num(dist, nn, R):
    num = [f'#{i + 1} [{dist[i]}]' if i != '-' else f'#{nn[0]}->#{nn[-1] + 1} Нет данных' for i in R]
    # num = [x for i, x in enumerate(num_all) if x not in num_all[:i]]
    return num


def split_backlog(name_data, data, dist, R):
    define = [name_data[dist[i]] == data[dist[i]].sort_values().dropna().values[0] if i != '-' else '-' for i in R]
    backlog = [eval(f'{"1" if define[i] == False else "-1"}') * abs(
        name_data[dist[i]] - data[dist[i]].sort_values().dropna().values[0 if define[i] == False or len(
            data[dist[i]].sort_values().dropna().values) < 2 else 1]) if i != '-' else '-' for i in R]
    percent_bk = [eval(f'{"1" if define[i] == False else "-1"}') * round(abs((name_data[dist[i]] * 100 / data[
        dist[i]].sort_values().dropna().values[0 if define[i] == False or len(
        data[dist[i]].sort_values().dropna().values) < 2 else 1]) - 100)) if i != '-' else '-' for i in R]
    return backlog, percent_bk


def find_nn(dist):
    if '-' in dist:
        nn = [n for n, i in enumerate(dist) if i == '-']
        delim = ['-'] * (nn[-1] - nn[0] + 1)
    else:
        nn = [len(dist)]
        delim = []
    return nn, delim


def split_leaders(data, dist, R):
    spl_leader = [f'{name_format(data.loc[:][dist[i]].sort_values().keys()[0])}' if i != '-' else '-' for i in R]
    return spl_leader


def split_place(name_data, data, dist, R):
    spl_place = [list(data[dist[i]].sort_values().values).index(name_data[dist[i]]) + 1 if i != '-' else '-' for i in R]
    return spl_place


def split_table_data(dist: list, data: DataFrame, name: str, group: str):
    name = f'{name.upper()}^{group.upper()}'
    dist = check_cp(dist, data, name)
    nn, delim = find_nn(dist)
    nn = sorted(nn)
    name_data = data.loc[name]
    if list(name_data.dropna().index) == ['RES']:
        mode = 0
        col = ['n', 'gt', 's', 'bk', 'p_bk', 'l', 's_p']
        return pd.DataFrame(columns=col), mode
    else:
        mode = 1
    try:
        R = [r if r not in range(nn[0], nn[-1] + 1) else '-' for r in range(len(dist))]
        # ПОДПИСИ К ПУНКТАМ
        num = find_num(dist, nn, R)
        # СПЛИТЫ
        split_time = splits_list(name_data, dist, R)
        # ОБЩЕЕ ВРЕМЯ
        general_times = general_times_list(name_data, dist, nn, delim)
        # ОТСТАВАНИЕ
        backlog, percent_bk = split_backlog(name_data, data, dist, R)
        # ЛИДЕР НА ПЕРЕГОНАХ
        spl_leader = split_leaders(data, dist, R)
        # МЕСТО НА ПЕРЕГОНЕ
        split_places = split_place(name_data, data, dist, R)
        # СОЗДАТЬ ФРЕЙМ С ДАННЫМИ
        col = ['n', 'gt', 's', 'bk', 'p_bk', 'l', 's_p']
        val = [num, general_times, split_time, backlog, percent_bk, spl_leader, split_places]
        SPL = {k: v for k, v in zip(col, val)}
        df = pd.DataFrame(SPL).set_index(['n'])
        return df, mode
    except:
        print('error with split data', name)
        mode = 0
        col = ['n', 'gt', 's', 'bk', 'p_bk', 'l', 's_p']
        return pd.DataFrame(columns=col), mode


def SPL(df: DataFrame, routes: dict, name, group, filt=''):
    data = find_data(df, filt, name, group, routes)
    dist = distance(routes=routes, name=name.upper(), group=group.upper())
    # фрейм с основными данными сплита
    spl, mode = split_table_data(dist, data, name, group)
    spl.drop_duplicates(inplace=True)
    spl = spl.fillna('-')
    if mode == 1:
        # Единичные данные (Стабильность, среднее отклонение, результат, результат абсолютного лидера)
        # stab, median, res, best_lead_res, best_lead_bk = general_route_info(name, group, spl['p_bk'], dist, data)
        # Собственные отставание, время, лучший результат
        # RVP, RVP_sum = self_bk(list(spl['s']), list(spl['bk']), spl['p_bk'].median())
        results_df, gen_res = results(df, group, routes)
        # split_to_file(name, res, RVP_sum, RVP, spl).to_excel(split_file)
        return spl, results_df
    else:
        print('ERROR', name)
        results_df = results(df, group, routes)


def split_to_file(name, res, RVP_sum, RVP, spl):
    print(f'{name} {format(res)} (Мог улучшить на: {format(RVP_sum[0])})')
    spl['bk'] = spl['bk'].apply(lambda x: format(x) if isinstance(x, str) == False else '-')
    spl['s'] = spl['s'].apply(lambda x: format(x) if isinstance(x, str) == False else '-')
    spl['gt'] = spl['gt'].apply(lambda x: format(x) if isinstance(x, str) == False else '-')
    spl['self_bk'] = [format(i) if isinstance(i, str) == False else '-' for i in RVP]
    spl['p_bk'] = spl['p_bk'].apply(lambda x: str(x) + '%')
    return spl


def calculate_stability_grade(values):
    values.replace(['-'], pd.NA, inplace=True)
    standard_deviation = values.std()
    stability = round(100 - standard_deviation) if 100 > standard_deviation else 20
    return stability


def general_route_info(name, group, p_backlog, dist, data):
    name = f'{name.upper()}^{group.upper()}'
    p_backlog.replace(['-'], pd.NA, inplace=True)
    # Результат
    res = data.loc[name]['RES'] if 'RES' in data.loc[name].index else null
    # Среднее отклонение по p_backlog
    median = round(p_backlog.median(), 2)
    # Время идеального лидера и отставание
    best_times = [pd.Timedelta(data[i].sort_values().values[0]) for i in dist]
    best_lead_res = pd.Series(best_times).sum()
    best_lead_bk = res - best_lead_res
    # Стабильность
    stab = calculate_stability_grade(p_backlog)
    return stab, median, res, best_lead_res, best_lead_bk


def self_bk(split_time, backlog, median):
    backlog1 = [i if i != '-' else pd.Timedelta(-1) for i in backlog]
    backlog2 = [i if i > null else null for i in backlog1]
    RVP = [i - ((i - j) * (1 + median / 100)) if i != '-' else '-' for i, j in zip(split_time, backlog2)]
    RVP_sum = pd.DataFrame([i for i in RVP if i != '-']).sum()
    return RVP, RVP_sum


def results(data, group, routes):
    names = [i for v in routes[group].values() for i in v]
    # Создание фрейма человек^группа : данные
    df = pd.DataFrame(index=names)
    df.index.name = 'name'
    # Заполнение результатов
    res_frame = data.loc[names]['RES']
    leader_res = res_frame.iloc[0]
    res = list(res_frame.values)
    # Заполнение отставаний
    backlogs = list(res_frame.apply(lambda x: x - leader_res if x != pd.NaT else pd.NaT))
    # Заполнение фрейма
    df['res'] = res
    df['l_bk'] = backlogs
    # Сбор рассевов этой группы
    group_routes = list(routes[group])
    # Люди с такими же дистанциями
    same_route_sportsmens = find_same_routes(group_routes, routes)
    # Результаты общей группы
    general_res_frame = pd.DataFrame(data.loc[same_route_sportsmens]['RES'].sort_values())
    leader_res2 = general_res_frame['RES'].values[0]
    general_res_frame['l_bk'] = list(
        general_res_frame['RES'].apply(lambda x: x - leader_res2 if x != pd.NaT else pd.NaT))
    return df, general_res_frame


def find_same_routes(group_routes, routes):
    groups = [gr for gr in routes for r in routes[gr] if r in group_routes]
    names = []
    for group in groups:
        names += [i for v in routes[group].values() for i in v]
    return list(set(names))
