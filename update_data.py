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
json_dir = dl_dir/'json'

def main():
    dl_dir.mkdir(exist_ok=True, parents=True)
    json_dir.mkdir(exist_ok=True, parents=True)
    data.get_season_gamelogs(season_yr=2021, season_type='Regular Season')
    return True


if __name__ == "__main__":
    main()
