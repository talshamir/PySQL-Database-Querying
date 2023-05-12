import requests
import SRC.CREATE_DB_SCRIPT.Create_DB_Script as db_script

API_KEY = "Bearer xvTk8OovqJo3MNZkHUNtdGbl6SJsZZBH8wCQxao/kq4dA+jB2UIW4CcX9tvvllke"
BASE_URL = "https://api.collegefootballdata.com"

playersByName = "/player/search?searchTerm={name}"
playerSeasonStats = "/stats/player/season?year={year}&category={category}"
playStats = "/play/stats?year={year}&team={team}"
draftPicks = "/draft/picks?year={year}"

headers = {"Authorization": API_KEY,
           "accept": "application/json"}

def get_draft_picks_by_year(year):
    full_url = BASE_URL + draftPicks.format(year=year)
    print("Done getting the data from the api: draft picks by year {year}".format(year = year))
    return requests.get(url=full_url, headers=headers).json()

def get_player_season_stats(year, category=""):
    full_url = BASE_URL + playerSeasonStats.format(year=year, category=category)
    print("Done getting the data from the api: season stats by year {year}".format(year = year))
    return requests.get(url=full_url, headers=headers).json()

def insert_json_to_table_in_bulk(json, table_name, **kwargs):
    # connect to the database
    db = db_script.connect_to_database()
    cursor = db.cursor()

    # get the columns names of the table
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [col[0] for col in cursor.fetchall()]
    # iterate through the data
    rows = []
    for item in json:
        keys = item.keys()
        keys = set(keys).union(set(kwargs.keys()))
        # get the intersection of the json keys and the table columns
        keys_intersection = list(set(keys) & set(columns))
        values = [item[key] if key not in kwargs.keys() else kwargs.get(key) for key in keys_intersection]
        if None in values:
            continue
        rows.append(values)
    # Remove duplicates before inserting into the table
    unique_list = list(set(tuple(row) for row in rows))
    unique_list = list(list(row) for row in unique_list)

    sql = f"INSERT INTO {table_name} ({','.join(keys_intersection)}) VALUES ({','.join(['%s'] * (len(keys_intersection)))}) ON DUPLICATE KEY UPDATE {','.join([f'{col}=VALUES({col})' for col in keys_intersection])}"
    cursor.executemany(sql, unique_list)
    db.commit()

    cursor.close()
    db.close()


def insert_player_stats_season_to_db_tables(num_of_years):
    for counter in range(num_of_years):
        current_year = str(2022 - counter)
        data = get_player_season_stats(year=current_year, category="receiving")

        insert_json_to_table_in_bulk(data, table_name="college_team_conference")
        insert_json_to_table_in_bulk(data, table_name="college_player")
        insert_json_to_table_in_bulk(data, table_name="college_player_stats", season=current_year)
        print("done inserting year" + current_year + " to all relevant tables")

def insert_nfl_draft_data_to_db_tables(num_of_years):
    for counter in range(num_of_years):
        current_year = str(2022 - counter)
        data = get_draft_picks_by_year(year=current_year)

        insert_json_to_table_in_bulk(data, table_name="draft_pick_college")
        insert_json_to_table_in_bulk(data, table_name="draft_pick_athlete")
        insert_json_to_table_in_bulk(data, table_name="draft_pick")
        print("done inserting year" + current_year + " to all relevant tables")


if __name__ == "__main__":
    # Getting data from the 2022 and 2021 seasons in college football.
    insert_player_stats_season_to_db_tables(2)
    insert_nfl_draft_data_to_db_tables(2)
    # the renaming should be done only after the data is inserted
    db_script.rename_columns("college_player", "player", "name")