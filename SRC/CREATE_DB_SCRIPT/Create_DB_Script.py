import mysql.connector

db_configuration = dict(Username="talshamir",
                        Database_name="talshamir",
                        Password="tals44146",
                        Host_name="localhost",
                        port="3305")


def connect_to_database():
    return mysql.connector.connect(
        host=db_configuration["Host_name"],
        user=db_configuration["Username"],
        password=db_configuration["Password"],
        database=db_configuration["Database_name"],
        port=db_configuration["port"])


def create_table(table_name, sql):
    try:
        connection = connect_to_database()
    except:
        print('Unable to connect, please check your configuration')
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    print(f"Table {table_name} created successfully.")


def create_tables():
    sql_create_college_team_conference = """
    CREATE TABLE college_team_conference (
        team VARCHAR(50) NOT NULL,
        conference VARCHAR(50) NOT NULL,
        PRIMARY KEY (team),
        INDEX team_index (team)
    );
    """
    create_table(table_name="college_team_conference", sql=sql_create_college_team_conference)

    sql_create_college_player = """
    CREATE TABLE college_player (
        playerId INT NOT NULL,
        player VARCHAR(50) NOT NULL,
        PRIMARY KEY (playerId),
        INDEX playerId_index (playerId)
    );
    """

    create_table(table_name="college_player", sql=sql_create_college_player)

    sql_create_college_player_stats = """
    CREATE TABLE college_player_stats (
        season INT NOT NULL,
        playerId INT NOT NULL,
        team VARCHAR(50) NOT NULL,
        category VARCHAR(50) NOT NULL,
        statType VARCHAR(20) NOT NULL,
        stat float(8),
        FOREIGN KEY (playerId) REFERENCES college_player(playerId),
        PRIMARY KEY (playerId, season, statType, team),
        INDEX playerId_index (playerId),
        INDEX statType_index (statType),
        INDEX team_index (team)
    );
    """

    create_table(table_name="college_players_stats", sql=sql_create_college_player_stats)

    # ------------------------------------------------------------------------------#

    sql_create_draft_pick_college = """
    CREATE TABLE draft_pick_college (
        collegeId INT NOT NULL,
        collegeTeam VARCHAR(50) NOT NULL,
        PRIMARY KEY (collegeId),
        INDEX collegeId_index (collegeId)
    );
    """

    create_table(table_name="picks_colleges", sql=sql_create_draft_pick_college)

    sql_create_draft_pick_athlete = """
    CREATE TABLE draft_pick_athlete (
        collegeAthleteId INT NOT NULL,
        position VARCHAR(50) NOT NULL,
        collegeId INT NOT NULL,
        PRIMARY KEY (collegeAthleteId),
        FOREIGN KEY (collegeId) REFERENCES draft_pick_college(collegeId),
        INDEX collegeAthleteId_index (collegeAthleteId)
    );
    """

    create_table(table_name="draft_pick_athlete", sql=sql_create_draft_pick_athlete)

    sql_create_draft_pick = """
    CREATE TABLE draft_pick (
        collegeAthleteId INT NOT NULL,
        nflTeam VARCHAR(50) NOT NULL,
        year INT NOT NULL,
        overall INT NOT NULL,
        FOREIGN KEY (collegeAthleteId) REFERENCES draft_pick_athlete(collegeAthleteId),
        PRIMARY KEY (overall, year),
        INDEX collegeAthleteId_index (collegeAthleteId),
        INDEX overall_index (overall)
    );
    """

    create_table(table_name="draft_picks", sql=sql_create_draft_pick)

    # ----------------------------------------------------------------------------------- #
    sql_create_nfl_player = """
    CREATE TABLE nfl_player (
        player_id VARCHAR(50) NOT NULL,
        player VARCHAR(50) NOT NULL,
        PRIMARY KEY (player_id),
        INDEX player_id_index (player_id)
    );
    """
    create_table(table_name="create_nfl_player ", sql=sql_create_nfl_player)

    sql_add_full_text_index = "ALTER TABLE nfl_player ADD FULLTEXT (player);"
    add_index(sql_add_full_text_index)

    sql_create_nfl_game = """
    CREATE TABLE nfl_game (
        Year INT NOT NULL,
        game_id VARCHAR(50) NOT NULL,
        PRIMARY KEY (game_id),
        INDEX game_id_index (game_id)
    );
    """

    create_table(table_name="create_nfl_game ", sql=sql_create_nfl_game)

    sql_create_nfl_receiving_per_game = """
    CREATE TABLE nfl_receiving_per_game (
        game_id VARCHAR(50) NOT NULL,
        player_id VARCHAR(50) NOT NULL,
        team VARCHAR(50) NOT NULL,
        targets INT,
        rec INT,
        rec_yds INT,
        rec_td INT,
        rec_long INT,
        FOREIGN KEY (game_id) REFERENCES nfl_game(game_id),
        FOREIGN KEY (player_id) REFERENCES nfl_player(player_id),
        PRIMARY KEY (game_id, player_id),
        INDEX game_id_index (game_id),
        INDEX player_id_index (player_id)
    );
    """

    create_table(table_name="nfl_receiving_per_game ", sql=sql_create_nfl_receiving_per_game)


def drop_all_tables():
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        # Get a list of all tables in the database
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        # Iterate over the tables and drop them
        for table in tables:
            cursor.execute(f"DROP TABLE {table[0]}")

        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

        conn.commit()
        print("All tables have been dropped.")
    except mysql.connector.Error as err:
        print(f"An error occurred: {err}")
    finally:
        cursor.close()
        conn.close()


def rename_columns(table_name, old_column_name, new_column_name):
    try:
        sql = "ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"
        conn = connect_to_database()
        cursor = conn.cursor()
        cursor.execute(
            sql.format(table_name=table_name, old_column_name=old_column_name, new_column_name=new_column_name))
        conn.commit()
        print(f"column {old_column_name} has been changed to {new_column_name} in table {table_name}")
        cursor.close()
        conn.close()
    except:
        print("Something went wrong, perhaps there is a mistake in the table name or old column name")

def add_index(sql):
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()
    connection.close()


if __name__ == "__main__":
    drop_all_tables()
    create_tables()

