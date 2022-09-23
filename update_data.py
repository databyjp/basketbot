# ========== (c) JP Hwang 2/2/2022  ==========

import logging
import pandas as pd
import data
import utils
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
    for season_yr in [2021, 2020]:
        season_gamelogs = data.fetch_season_gamelogs(season_yr=season_yr, season_type='Regular Season', use_local=True)
        print(len(season_gamelogs))
        season_pbps = data.fetch_season_pbps(season_yr=season_yr, season_type='Regular Season')
        print(len(season_pbps))
    return True


if __name__ == "__main__":
    main()
