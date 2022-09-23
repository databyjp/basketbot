# ========== (c) JP Hwang 2/2/2022  ==========

import logging
import pandas as pd
import os
import json
import utils
import time

logger = logging.getLogger(__name__)


class GamelogData:
    """
    Manage gamelogs data from NBA API.
    """
    def __init__(self, content, season_yr, season_type, set_name=None):
        """
        :param content: JSON payload
        :param season_yr: e.g. "2021-22"
        :param season_type: 'Regular season' or 'Playoffs'
        :param set_name: e.g. 'ATL' or 'NBA'
        """
        self.result_sets = content["resultSets"]
        self.data_type = content['resource']
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
        out_path = os.path.join(utils.dl_dir, self.fname)
        data_df.to_csv(out_path, index=False)
        return True

    def __repr__(self):
        return f'{self.data_type} data for {self.set_name} in {self.season_suffix} season'


def fetch_team_gamelogs(team_abv=None, season_yr=None, season_type=None, use_local=False, dl_dir=None):
    """
    Downloads game logs for a team for a given season
    :param team_abv: Team Abbreviation
    :param season_yr: Starting year of the season to get (e.g. 2022 for '22-'23)
    :param season_type: Season type (^(Regular Season)|(Pre Season)|(Playoffs)|(All-Star)|(All Star)$)
    :param use_local: Use local file if available (set False if getting data for season in progress)
    :return:
    """
    from nba_api.stats.endpoints import teamgamelogs

    teams_dict = utils.get_teams_dict()
    team_id = teams_dict[team_abv]

    if season_yr is None:
        season_yr = utils.curr_season_yr()
    season_suffix = utils.year_to_season_suffix(season_yr)

    fname = utils.get_fname('gamelogs', season_suffix, season_type, set_name=team_abv)
    if dl_dir is None:
        dl_dir = utils.dl_dir

    if use_local and (dl_dir/fname).exists():
        return pd.read_csv(dl_dir/fname)

    else:
        response = teamgamelogs.TeamGameLogs(
            team_id_nullable=str(team_id), season_nullable=season_suffix, season_type_nullable=season_type,
        )
        content = json.loads(response.get_json())
        team_gamelogs = GamelogData(content, season_yr, season_type, set_name=team_abv)
        team_gamelogs.to_csv()
        time.sleep(60/utils.max_req_per_min)  # Only sleep if API call made

    return team_gamelogs.to_df()


def fetch_season_gamelogs(season_yr=None, season_type=None, use_local=False, dl_dir=None):
    """
    Downloads & save team game logs for a given season
    This will always overwrite data.  # TODO - only overwrite data for the current and the immediately prev. season;
    :param season_yr: Starting year of the season to get (e.g. 2022 for '22-'23)
    :param season_type: Season type (^(Regular Season)|(Pre Season)|(Playoffs)|(All-Star)|(All Star)$)
    :param use_local: Use local file if available (set False if getting data for season in progress)
    :return:
    """
    team_list = pd.read_csv("data/team_id_list.csv")
    if season_yr is None:
        season_yr = utils.curr_season_yr()

    team_gamelogs_list = list()
    for i, row in team_list.iterrows():
        team_abv = row["TEAM_ABBREVIATION"]
        team_gamelogs = fetch_team_gamelogs(team_abv=team_abv, season_yr=season_yr, season_type=season_type, use_local=use_local, dl_dir=dl_dir)
        if len(team_gamelogs) > 0:
            logger.info(f"Fetched {season_type} data for {team_abv} in {season_yr}.")
            team_gamelogs_list.append(team_gamelogs)
        else:
            logger.warning(f"Warning: no {season_type} data found for {team_abv} in {season_yr}.")

    return team_gamelogs_list


def load_team_gamelogs(team_abv=None, season_yr=None, season_type='Regular Season', dl_dir=None):
    """
    Load locally saved game log (box score) data for a team
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

    if dl_dir is None:
        dl_dir = utils.dl_dir

    gl_df_list = list()
    for team_abv in team_abv_list:
        fname = utils.get_fname('gamelogs', season_suffix, season_type, set_name=team_abv)
        fpath = dl_dir/fname
        tm_gl_df = pd.read_csv(fpath)
        gl_df_list.append(tm_gl_df)
    gl_df = pd.concat(gl_df_list)

    return gl_df


def fix_gm_id(gm_id):
    if str(gm_id)[:2] != '00':
        gm_id = '00' + str(gm_id)  # This accounts for inconsistencies in NBA's game ID naming system.
    return gm_id


def get_gamedata_path(gm_id, datatype, gamedata_dir=None):
    """
    Convert NBA's game ID to file path
    :param gm_id: NBA-specified game id
    :param gamedata_dir: Directory of file location (should be a pathlib.Path obj)
    :return:
    """
    if gamedata_dir is None:
        gamedata_dir = utils.rawdata_dir

    gm_id = fix_gm_id(gm_id)
    gamedata_fname = f'{gm_id}_{datatype}.json'
    return gamedata_dir/gamedata_fname


def fetch_gamedata(gm_id, datatype="pbp", gamedata_dir=None):
    """
    Download a datafile based on gameID as downloaded from NBA API & saves to file
    :param gm_id: NBA game ID
    :param datatype: What data types to download - determines endpoint to use
    :param gamedata_dir: Directory of file location (should be a pathlib.Path obj)
    :return:
    """
    from nba_api.stats.endpoints import boxscoreadvancedv2
    from nba_api.live.nba.endpoints import playbyplay
    import time

    if datatype not in ['boxscore', 'pbp']:
        logger.warning(f'Supplied datatype ({datatype}) not recognised; defaulting to box score.')
        datatype = 'boxscore'

    gamedata_path = get_gamedata_path(gm_id, datatype, gamedata_dir)

    if gamedata_path.exists():
        logger.info(f"JSON found for game {gm_id}, reading local file.")
        with open(gamedata_path, 'r') as f:
            content = json.load(f)
    else:
        try:
            logger.info(f"Downloading {datatype} data for game {gm_id}")
            gm_id = fix_gm_id(gm_id)
            if datatype == "boxscore":
                response = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=gm_id)
            else:
                response = playbyplay.PlayByPlay(gm_id)

            content = json.loads(response.get_json())
            with open(gamedata_path, 'w') as f:
                json.dump(content, f)
            logger.info(f"Got {datatype} data for game {gm_id}")
        except:
            logger.exception(f"Error getting {datatype} data for game {gm_id}")
            content = None
        time.sleep(60/utils.max_req_per_min)  # Only sleep if API call made

    return content


def pbp_content_to_df(content):
    pbp_df = pd.DataFrame(content['game']['actions'])
    pbp_df["GAME_ID"] = content["game"]['gameId']
    pbp_df = pbp_df.assign(realtime_dt=pd.to_datetime(pbp_df["timeActual"]))
    return pbp_df


def load_pbp_data(gm_id_list, gamedata_dir=None):
    """
    Load game data based on gameID as downloaded from NBA API & saves to file
    :param gm_id_list: NBA game IDs
    :param datatype: What data types to download - determines endpoint to use
    :param gamedata_dir: Directory of file location (should be a pathlib.Path obj)
    :return:
    """

    pbp_df_list = list()
    for gm_id in gm_id_list:
        content = fetch_gamedata(gm_id, datatype='pbp', gamedata_dir=gamedata_dir)
        if content is not None:
            gm_pbp_df = pbp_content_to_df(content)
            pbp_df_list.append(gm_pbp_df)
    pbp_df = pd.concat(pbp_df_list)

    return pbp_df


def fetch_season_pbps(season_yr=None, season_type='Regular Season', dl_dir=None):
    """

    :param season_yr:
    :param season_type:
    :return:
    """
    """
    NOTE: The NBA_API for "live" pbp data seems to be missing many files from before 2020-21 season. 
    (Needs To be investigated further)
    There are other pbp endpoints available, but they do not include as much information as the "live" endpoint.
    """
    if season_yr is None:
        season_yr = utils.curr_season_yr()
    if dl_dir is None:
        dl_dir = utils.dl_dir

    gl_df = load_team_gamelogs(team_abv='NBA', season_yr=season_yr, season_type=season_type)
    pbp_df = load_pbp_data(gl_df['GAME_ID'])
    pbp_path = dl_dir/f'pbp_{season_yr}.csv'
    pbp_df.to_csv(pbp_path, index=False)
    return pbp_df
