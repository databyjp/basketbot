# BasketBot

An easy-to-use wrapper for [nba_api](https://github.com/swar/nba_api) to speed up downloading NBA data. 

## What is it?
People sometimes ask where they can get up-to-date basketball data, so I thought I would clean some of my code & share it. 

The idea is to build an **easy-to-use** library which people can use to download high-quality, up-to-date data with just a line or two of code.

For now, it only supports getting the play-by-play data (including some event locations) and team box score data. 

## How do I use it?
For now it's pretty basic. To try it out:
- Clone the repo
- Set up a virtual environment using `requirements.txt`
- Activate the virtual environment
- Run scripts using syntax `python [script_name] [start_year] [end_year|OPTIONAL]`
  - Run `python download_pbp_data.py 2020 2021` to download the 2020-21 and 2021-22 play-by-play and team box score data.
  - Run `python download_pbp_data.py 2020` to download the 2020-21 play-by-play and team box score data.
  - Run `python download_boxscores.py 2021` to download the 2021-22 player box score data.
