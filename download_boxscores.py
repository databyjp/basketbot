# ========== (c) JP Hwang 30/9/2022  ==========

import logging
import pandas as pd
import data
import utils
import sys

# ===== SET UP LOGGER =====
logger = logging.getLogger(__name__)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
root_logger.addHandler(sh)
# ===== END LOGGER SETUP =====

desired_width = 320
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', desired_width)

dl_dir = utils.dl_dir
rawdata_dir = utils.rawdata_dir
datatype = 'boxscore'


def main():
    dl_dir.mkdir(exist_ok=True, parents=True)
    rawdata_dir.mkdir(exist_ok=True, parents=True)

    example_query = utils.example_query

    if len(sys.argv) < 2:
        print(f'Sorry, I need more information. {example_query}')
        return False
    else:
        if len(sys.argv) == 2:
            yr_a = int(sys.argv[1])
            yr_b = int(sys.argv[1])
        else:
            yr_a = int(sys.argv[1])
            yr_b = int(sys.argv[2])

        try:
            if utils.curr_season_yr() > yr_a > 1980 and utils.curr_season_yr() > yr_b > 1980:
                yr_ranges = [yr_a, yr_b]
                yr_ranges.sort()
                print(f'Great, fetching data between {yr_ranges[0]} and {yr_ranges[1]}! (Inclusive.)')
            else:
                print(f'Sorry, the arguments should be years (e.g. 2021 2020) when data is available. {example_query}')
                return False
        except:
            print('Sorry. The arguments should be years (e.g. 2021 2020). ' + example_query)
            return False

    for season_yr in list(range(yr_ranges[0], yr_ranges[1]+1)):
        season_gamelogs = data.fetch_season_gamelogs(season_yr=season_yr, season_type='Regular Season', use_local=True, dl_dir=dl_dir)
        season_gamelog_df = pd.concat(season_gamelogs)

        print(len(season_gamelogs))

        box_dfs = list()
        for gm_id in season_gamelog_df['GAME_ID'].to_list():
            box_score = data.fetch_gamedata(gm_id, datatype=datatype, gamedata_dir=None)
            box_df = pd.DataFrame(box_score['resultSets'][0]['rowSet'], columns=box_score['resultSets'][0]['headers'])
            box_dfs.append(box_df)
        box_df = pd.concat(box_dfs)
        box_path = dl_dir / f'boxscores_{season_yr}.csv'
        box_df.to_csv(box_path, index=False)

    return True


if __name__ == "__main__":
    main()

