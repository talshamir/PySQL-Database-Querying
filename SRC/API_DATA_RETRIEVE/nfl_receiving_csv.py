import csv
import SRC.CREATE_DB_SCRIPT.Create_DB_Script as db_script


def insert_csv_to_table_in_bulk(csv_file, table_name):
    # Connect to the MySQL database
    try:
        connection = db_script.connect_to_database()
    except:
        print('Unable to connect, please check your configuration')

    cursor = connection.cursor()

    cursor.execute(f"DESCRIBE {table_name}")
    table_columns = [col[0] for col in cursor.fetchall()]
    # Open the CSV file
    with open(csv_file, "r") as f:
        reader = csv.reader(f)

        columns_names = next(reader)
        columns_names[0] = columns_names[0][3:]
        columns_to_insert = list(set(columns_names).intersection(table_columns))
        columns_indices = {}
        for index, col in enumerate(columns_names):
            if col in columns_to_insert:
                columns_indices[col] = index

        # Prepare the SQL insert statement
        sql = f"INSERT INTO {table_name} ({','.join(columns_to_insert)}) VALUES ({','.join(['%s'] * (len(columns_to_insert)))}) ON DUPLICATE KEY UPDATE {','.join([f'{col}=VALUES({col})' for col in columns_to_insert])}"
        rows = []
        for row in reader:
            rows.append([row[columns_indices[col]] for col in columns_to_insert])
        # Insert the data into the MySQL database in bulk
        cursor.executemany(sql, rows)

    # Close the cursor and connection
    cursor.close()
    connection.commit()
    connection.close()

if __name__ == "__main__":
    insert_csv_to_table_in_bulk("nfl_reciving_yards_clean.csv", table_name="nfl_game")
    insert_csv_to_table_in_bulk("nfl_reciving_yards_clean.csv", table_name="nfl_player")
    insert_csv_to_table_in_bulk("nfl_reciving_yards_clean.csv", table_name="nfl_receiving_per_game")
    # the renaming should be done only after the data is inserted
    db_script.rename_columns("nfl_receiving_per_game", "team", "nfl_team")
    db_script.rename_columns("nfl_receiving_per_game", "rec", "receptions")
    db_script.rename_columns("nfl_receiving_per_game", "rec_yds", "reception_yards")
    db_script.rename_columns("nfl_receiving_per_game", "rec_td", "touchdowns")
    db_script.rename_columns("nfl_receiving_per_game", "rec_long", "longest_reception")
    db_script.rename_columns("nfl_player", "player", "name")
