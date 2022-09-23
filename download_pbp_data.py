# ========== (c) JP Hwang 2/2/2022  ==========

import logging
import pandas as pd
import data
import utils
import sys
from pathlib import Path

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


def main():
    dl_dir.mkdir(exist_ok=True, parents=True)
    rawdata_dir.mkdir(exist_ok=True, parents=True)

    example_query = 'Try something like: \n========================================\n$ python update_data.py [START_YEAR] [END YEAR]\n========================================\n\n'

    if len(sys.argv) < 3:
        print(f'Sorry, I need more information. {example_query}')
    else:
        try:
            if utils.curr_season_yr() > int(sys.argv[1]) > 1980 and utils.curr_season_yr() > int(sys.argv[2]) > 1980:
                yr_ranges = [int(sys.argv[1]), int(sys.argv[2])]
                yr_ranges.sort()
                print(f'Great, fetching data between {yr_ranges[0]} and {yr_ranges[1]}! (Inclusive.)')
            else:
                print(f'Sorry, the arguments should be years (e.g. 2021 2020) when data is available. {example_query}')
        except:
            print('Sorry. The arguments should be years (e.g. 2021 2020). ' + example_query)

    for season_yr in list(range(yr_ranges[0], yr_ranges[1]+1)):
        season_gamelogs = data.fetch_season_gamelogs(season_yr=season_yr, season_type='Regular Season', use_local=True, dl_dir=dl_dir)
        print(len(season_gamelogs))
        if season_yr >= 2020:
            season_pbps = data.fetch_season_pbps(season_yr=season_yr, season_type='Regular Season', dl_dir=dl_dir)
            print(len(season_pbps))
        else:
            print('Sorry, we have yet to enable support for data older than 2020-21.')

    return True


if __name__ == "__main__":
    main()
