# ========== (c) JP Hwang 2/2/2022  ==========

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Parameters
file_prefixes = {"pl_list": "common_all_players", "gamelogs": "gamelogs",
                 "proc_pbp": "proc_pbp", "shots_pbp": "shots_pbp"}
dl_dirpath = "dl_data"
dl_dir = Path(dl_dirpath)
rawdata_dir = dl_dir/'raw_gamedata'
max_req_per_min = 30

def_start_year = 2015  # Default start year for multi-year based functions


def get_fname(filetype, season_suffix, season_type, set_name=None):
    """
    Generate consistent filenames for saving/loading files
    :param filetype:
    :param season_suffix: "2021-22" etc.
    :param season_type: "Regular season", "Playoffs"
    :return:
    """
    po_suffix = ''
    setname_str = ''

    if season_type == 'Playoffs':
        po_suffix = '_playoffs'

    if set_name is not None:
        setname_str = set_name + '_'

    return f"{file_prefixes[filetype]}_{setname_str}{season_suffix}{po_suffix}.csv"


def curr_season_yr():
    """
    Return the current season year (e.g. 2021 if Mar 2022 as still part of 2021-22 season, 2022 if Oct 2022)
    :return:
    """
    from datetime import datetime

    cur_yr = datetime.now().year
    if datetime.now().month <= 8:
        cur_yr -= 1
    return cur_yr


def year_to_season_suffix(season_yr):
    """
    Convert year to season suffix as used in the API
    * Note - will not work for pre-2000
    :param season_yr: Season - year in integer (first year in common season name e.g. 2020 for 2020-21)
    :return: Season suffix as string
    """
    return f"{season_yr}-{str(season_yr + 1)[-2:]}"


def get_teams_dict():
    teams_list = pd.read_csv("data/team_id_list.csv")
    teams_dict = dict(zip(teams_list['TEAM_ABBREVIATION'], teams_list['TEAM_ID']))
    return teams_dict
