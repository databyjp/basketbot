# ========== (c) JP Hwang 2/2/2022  ==========

import logging
import pandas as pd
import os
import json
import utils
import time

logger = logging.getLogger(__name__)

dl_dir = utils.dl_dir


class GamelogData:
    """
    Manage gamelogs data from NBA API.
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

    teams_dict = utils.get_teams_dict()
    team_id = teams_dict[team_abv]

    if season_yr is None:
        season_yr = utils.curr_season_yr()
    season_suffix = utils.year_to_season_suffix(season_yr)

    response = teamgamelogs.TeamGameLogs(
        team_id_nullable=str(team_id), season_nullable=season_suffix, season_type_nullable=season_type,
    )
    content = json.loads(response.get_json())
    team_gamelogs = GamelogData(content, 'team_gamelogs', season_yr, season_type, set_name=team_abv)
    return team_gamelogs


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

    team_gamelogs_list = list()
    for i, row in team_list.iterrows():
        team_abv = row["TEAM_ABBREVIATION"]
        team_gamelogs = get_team_gamelogs(team_abv=team_abv, season_yr=season_yr, season_type=season_type)
        if len(team_gamelogs.result_sets[0]['rowSet']) > 0:
            logger.info(f"Fetched {season_type} data for {team_abv} in {season_yr}.")
            team_gamelogs.to_csv()
            team_gamelogs_list.append(team_gamelogs)
        else:
            logger.warning(f"Warning: no {season_type} data found for {team_abv} in {season_yr}.")
        time.sleep(5)

    return team_gamelogs_list


def load_team_gamelogs(team_abv=None, season_yr=None, season_type=None):
    """
    Load game log (box score) data for a team
    :param team_abv: Team abbreviation
    :param season_yr: Year to load data in (e.g. 2020 for 2020-21 season)
    :param season_type: Season types (see def_season_types)
    :return:
    """
    season_suffix = utils.year_to_season_suffix(season_yr)
    if team_abv == 'NBA':
        team_abv_list = utils.get_teams_dict().keys()
    else:
        team_abv_list = [team_abv]

    gl_df_list = list()
    for team_abv in team_abv_list:
        fname = utils.get_fname('team_gamelogs', season_suffix, season_type, set_name=team_abv)
        fpath = dl_dir/fname
        tm_gl_df = pd.read_csv(fpath)
        gl_df_list.append(tm_gl_df)
    gl_df = pd.concat(gl_df_list)

    return gl_df
