import random
import pandas as pd
from pandas import Timedelta
from Backend.EVENT_parser.common_functions import fill_GD, dispersions, null, bs, rl, course
from bs4 import BeautifulSoup as BS
from bs4.element import ResultSet
import re

global log


def soup_main_info(soup: BS):
    h2: list = [i.get_text(' ').upper() for i in soup.find_all('h2')]
    tables: ResultSet = soup.find_all('table')
    return h2, tables


def check_void_groups(soup: BS):  # Count the number of people in each group
    h2, tables = soup_main_info(soup)
    number_of_sportsmens: dict = {h2[i]: len(bs(tables[i]).find_all('tr')) - 1 for i in rl(h2)}
    return number_of_sportsmens


def BS_group_data(group: str, soup: BS):  # Returns the soup with the table (can be sent to name and split handlers)
    h2, tables = soup_main_info(soup)
    group_soup: BS = bs(tables[h2.index(group)])
    tr = group_soup.find_all('tr')
    th = group_soup.find_all('th')
    return group_soup, tr, th


def group_names(group: str, tr: ResultSet):  # Remove group name truncation .split(' ')[0]
    tr_pt: BS = bs(tr[1])
    td_pr: ResultSet = tr_pt.find_all('td', class_='cr')
    all_td: ResultSet = bs(''.join(str(tr))).find_all('td', class_='cr')
    td: list = [all_td[i].get_text(' ') if all_td[i].get_text("") != '' else f"ND{i}" for i in rl(all_td)]
    if " " in td_pr[0].get_text(' ') or "\n" in td_pr[0].get_text(' '):
        names: list = [i.upper() + f"^{group.upper()}" for i in td[::len(td_pr)]]
    elif len(td_pr) == 3:
        d_ind: list = [j * 3 - 1 for j in range(1, len(td))]
        names: list = [td[i] for i in rl(td) if i not in d_ind]
        names: list = [" ".join(
            names[i:i + 2]).upper() + f"^{group.upper()}" \
                       for i in rl(names)][:-1:2]
    elif len(td_pr) == 4:
        names: list = [td[i].upper() + ' ' + td[i + 1].upper() + f"^{group.upper()}" for
                       i in range(len(td) - 1)][::2][::2]
    elif len(td_pr) == 2 and " " not in td_pr[0].get_text(' ') or "\n" in td_pr[0].get_text(' '):
        names: list = [i.upper() + f"^{group.upper()}" for i in
                       td[::len(td_pr)]]
    else:
        raise ValueError('Names not found')
    names = [re.sub(' +', ' ', i) for i in names]
    names = [i if names.count(i) == 1 else f'{i.split("^")[0]}*{random.randint(0, 100)}^{group.upper()}' for i in names]
    return names


""" Check protocols for the presence of control points """


def group_results(tr: ResultSet, names: list):  # Returns a dictionary name - result
    f = lambda x: re.search('\d+:\d+:\d+', x)
    s = [f(str(i)).group() if f(str(i)) is not None else '00:00:00' for i in tr[1:]]
    all_res = [Timedelta(hours=int(i.split(':')[0]), minutes=int(i.split(':')[1]),
                         seconds=int(i.split(':')[2])) if ':' in i else null for i in s]
    return dict(zip(names, all_res))


def points(tr: ResultSet, th: ResultSet, tm_index: int,
           names: list):  # Check for the presence of seeding, return points for each person
    r = rl(tr)[:-1]
    if len(th[tm_index].get_text(' ')) > 3:
        cp = lambda s: int(re.search('\(\d{1,3}\)', s).group()[1:-1])
        disp: list = [241] + [cp(i.text) for i in th[tm_index:]]
        return dict(zip(names, [course(disp) for i in r]))
    f = lambda x: re.search('\[\d{2,3}\]', str(x))
    l: list = []
    for j in r:
        s = bs(tr[j + 1]).find_all('td')
        dist = [241] + [int(f(i).group()[1:-1]) if f(i) is not None else 00 for i in s[tm_index:-1]] + [240]
        l.append(course(dist))
    return dict(zip(names, l))


""" WORK WITH INDIVIDUAL """


def times(tr: ResultSet, tm_index: int):
    times = []
    for i in rl(tr):
        td: ResultSet = bs(tr[i]).find_all('td')
        f = lambda x: re.search('\d+:\d{2,3}', x).group()
        t: list = [f(i.get_text(' ')) if i.text != '' else '00:00' for i in td[tm_index:]]
        t_delta: list = [null] + [Timedelta(minutes=int(i.split(':')[0]), seconds=int(i.split(':')[1])) for i in t]
        times.append(t_delta)
    return times


def splits(times: list, names: list):
    spl = []
    for l in times:
        f = lambda x, i: x[i] - x[i - 1] if x[i] > x[i - 1] else null
        t: list = [f(l, i) for i in range(1, len(l))]
        spl.append(t)
    ret = dict(zip(names, spl))
    return ret


def routes_and_splits_dict(routes: list, splits: list):
    if len(routes) == len(splits):
        d: dict = dict(zip(routes, splits))
        return d
    else:
        raise ValueError(f'{len(routes), len(splits)} lengths are not the same')


""" GROUP PROCESSING """


def results_and_group_general_data(group: str, soup):
    group_soup, tr, th = BS_group_data(group.upper(), soup)
    tm_index = [th.index(i) for i in th if "#" in i.get_text()]
    names = group_names(group.upper(), tr)
    results = group_results(tr, names)
    return group_soup, tr, th, tm_index, names, results


def times_and_points(tr, th, tm_index, names, results):
    try:
        tm_index = tm_index[0]
    except IndexError:
        disps = {'RES': names}
        return pd.DataFrame(results, index=['RES']).T, disps
    try:
        points_l = points(tr, th, tm_index, names)
        times_l = times(tr[1:], tm_index)
    except AttributeError:
        return pd.DataFrame(results, index=['RES']).T
    return times_l, points_l


def SFR_parsing_data(group: str, soup: BS):
    group_soup, tr, th, tm_index, names, results = results_and_group_general_data(group.upper(), soup)
    times_l, points_l = times_and_points(tr, th, tm_index, names, results)
    splits_l = splits(times_l, names)
    return names, points_l, splits_l, results


def group_frame(group: str, soup: BS):
    names, points_l, splits_l, results = SFR_parsing_data(group, soup)
    GD = fill_GD(names, points_l, splits_l, results)
    disps = dispersions(points_l)
    df = pd.DataFrame(GD).T
    return df, disps


def SFR_parsing(sp):
    h2, tb = soup_main_info(sp)
    global log
    try:
        check = check_void_groups(sp)
    except IndexError:
        log = 'index error'
        return pd.DataFrame(), {}, log
    groups = [i.upper() for i in h2 if check[i] != 0]

    df = pd.DataFrame()
    routes = {}
    for group in groups:
        try:
            group_df, disps = group_frame(group, sp)
        except:
            group_df = pd.DataFrame()
            disps = {}
            log = 'group frame error'
            return pd.DataFrame(), {}, log
        df = pd.concat([df, group_df], join='outer', axis=0)
        routes.setdefault(group, disps)
    log = 'correct'
    df = df.replace([null], pd.NaT)
    # df = df.dropna(axis=1, how='all')
    print(log, df.shape, len(routes))
    return df, routes, log


