# ========== (c) JP Hwang 2/2/2022  ==========

import logging
import pandas as pd
import os
import json
import utils
import time

logger = logging.getLogger(__name__)

dl_dir = utils.dl_dir


class NBAData:
    """
    Generic class to represent returned payload from NBA API, with methods defined to make them easier to handle.
    """
    def __init__(self, content, data_type, season_yr, season_type, set_name=None):
        self.result_sets = content["resultSets"]
        self.data_type = data_type
        self.season_yr = season_yr
        self.season_type = season_type
        self.set_name = set_name  # 'NBA', 'BOS', etc.

        self.season_suffix = utils.year_to_season_suffix(self.season_yr)
        self.fname = utils.get_fname(self.data_type, self.season_suffix, self.season_type, self.set_name)

    def to_df(self):
        df_list = list()
        for i in range(len(self.result_sets)):
            results = self.result_sets[i]
            headers = results["headers"]
            rows = results["rowSet"]
            if len(rows) > 0:
                tdf = pd.DataFrame(rows)
                tdf.columns = headers
                df_list.append(tdf)
        if len(df_list) > 0:
            return pd.concat(df_list)
        else:
            logger.warning('This appears to be an empty dataset.')
            return None

    def to_csv(self):
        data_df = self.to_df()
        out_path = os.path.join(dl_dir, self.fname)
        data_df.to_csv(out_path, index=False)
        return True

    def __repr__(self):
        return f'{self.data_type} data for {self.set_name} in {self.season_suffix} season'


def get_team_gamelogs(team_abv=None, season_yr=None, season_type=None):
    """
    Downloads game logs for a team for a given season
    :param team_abv: Team Abbreviation
    :param season_yr: Starting year of the season to get (e.g. 2022 for '22-'23)
    :param season_type: Season type (^(Regular Season)|(Pre Season)|(Playoffs)|(All-Star)|(All Star)$)
    :return:
    """
    from nba_api.stats.endpoints import teamgamelogs

    team_list = pd.read_csv("data/team_id_list.csv")
    team_dict = dict(zip(team_list['TEAM_ABBREVIATION'], team_list['TEAM_ID']))
    team_id = team_dict[team_abv]

    if season_yr is None:
        season_yr = utils.curr_season_yr()
    season_suffix = utils.year_to_season_suffix(season_yr)

    response = teamgamelogs.TeamGameLogs(
        team_id_nullable=str(team_id), season_nullable=season_suffix, season_type_nullable=season_type,
    )
    content = json.loads(response.get_json())
    team_data = NBAData(content, 'team_gamelogs', season_yr, season_type, set_name=team_abv)
    return team_data


def get_season_gamelogs(season_yr=None, season_type=None):
    """
    Downloads & save team game logs for a given season
    :param season_yr: Starting year of the season to get (e.g. 2022 for '22-'23)
    :param season_type: Season type (^(Regular Season)|(Pre Season)|(Playoffs)|(All-Star)|(All Star)$)
    :return:
    """
    team_list = pd.read_csv("data/team_id_list.csv")
    if season_yr is None:
        season_yr = utils.curr_season_yr()

    team_data_list = list()
    for i, row in team_list.iterrows():
        team_abv = row["TEAM_ABBREVIATION"]
        team_data = get_team_gamelogs(team_abv=team_abv, season_yr=season_yr, season_type=season_type)
        team_data_list.append(team_data)
        if len(team_data.result_sets[0]['rowSet']) > 0:
            logger.info(f"Fetched {season_type} data for {team_abv} in {season_yr}.")
        else:
            logger.warning(f"Warning: no {season_type} data found for {team_abv} in {season_yr}.")
        time.sleep(5)

    return team_data_list


def load_team_gamelogs(st_year=None, end_year=None, season_types=None):
    """
    Load game log (box score) data
    :param st_year: Year to load data from (e.g. 20 for 2020-21 season)
    :param end_year: Year to load data to (e.g. 21 for 2021-22 season)
    :param season_types: Season types (see def_season_types)
    :return:
    """
    if season_types is None:
        season_types = ["Regular Season", "Playoffs"]

    if st_year is None:
        st_year = utils.def_start_year
    if end_year is None:
        end_year = utils.curr_season_yr() + 1

    gldf_list = list()
    for yr in range(st_year, end_year+1):
        for season_type in season_types:
            season_suffix = utils.year_to_season_suffix(yr)
            fname = utils.get_fname('team_gamelogs', season_suffix, season_type)
            fpath = os.path.join("dl_data", fname)
            if os.path.exists(fpath):
                t_df = pd.read_csv(fpath, dtype={"GAME_ID": "str"})
                gldf_list.append(t_df)
            else:
                logger.warning(f"File not found at {fpath}")
    gldf = pd.concat(gldf_list)
    gldf = gldf.assign(gamedate_dt=pd.to_datetime(gldf["GAME_DATE"]))
    return gldf
