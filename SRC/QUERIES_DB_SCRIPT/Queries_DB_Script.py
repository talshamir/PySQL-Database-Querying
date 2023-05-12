import SRC.CREATE_DB_SCRIPT.Create_DB_Script as db_script

def execute_query(query):
    print("Executing query")
    connection = db_script.connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    column_names = tuple([column[0] for column in cursor.description])
    print(column_names)
    result = cursor.fetchall()
    for row in result:
        print(row)
    cursor.close()
    connection.close()
    print("Done executing query")
    print("-"*60)


def get_the_top5_colleges_who_has_the_most_first_round_picks_in_the_draft():
    sql = """ SELECT collegeTeam, count(draft_pick_athlete.collegeAthleteId) as players_in_draft
                FROM draft_pick
                JOIN draft_pick_athlete
                ON draft_pick.collegeAthleteId = draft_pick_athlete.collegeAthleteId
                JOIN draft_pick_college
                ON draft_pick_college.collegeId = draft_pick_athlete.collegeId
                WHERE overall <= 32
                GROUP BY draft_pick_college.collegeTeam
                ORDER BY players_in_draft DESC
                LIMIT 5"""
    execute_query(sql)

def get_leading_receivers_in_the_last_two_seasons():
    sql = """SELECT college_player_stats.playerId, college_player.name, SUM(CASE statType 
                                                                            WHEN 'TD' THEN stat*15 
                                                                            WHEN 'YDS' THEN stat 
                                                                            WHEN 'LONG' THEN stat*5 
                                                                            ELSE 0 
                                                                            END) AS rating
            FROM college_player_stats
            JOIN college_player
            ON college_player_stats.playerId = college_player.playerId
            GROUP BY college_player_stats.playerId, college_player.name
            HAVING rating > 1000
            ORDER BY rating DESC"""
    execute_query(sql)

def most_yards_by_the_leading_reciver_in_each_conference():
    sql = """SELECT college_team_conference.conference, max(stat) as most_yards_by_leading_reciver
             FROM college_player
             JOIN college_player_stats
             ON college_player_stats.playerId = college_player.playerId
             JOIN college_team_conference
             ON college_team_conference.team = college_player_stats.team
             WHERE college_player_stats.statType = "YDS" AND
             college_player.name != ' Team'
             GROUP BY college_team_conference.conference
             ORDER BY most_yards_by_leading_reciver DESC"""
    execute_query(sql)

def compare_nfl_and_colleges_by_receivers():
    sql =   """(SELECT name, SUM(nfl_receiving_per_game.reception_yards) AS total_yards, ("NFL") AS League
                FROM nfl_receiving_per_game
                JOIN nfl_player
                ON nfl_receiving_per_game.player_id = nfl_player.player_id
                JOIN nfl_game
                ON nfl_game.game_id = nfl_receiving_per_game.game_id
                WHERE Year = 2021
                GROUP BY nfl_receiving_per_game.player_id
                ORDER BY total_yards DESC
                LIMIT 5)
                UNION
                (SELECT name, stat as total_yards, ("NCAA") AS League
                FROM  college_player_stats
                JOIN college_player
                ON college_player.playerId = college_player_stats.playerId
                WHERE season = 2021
                AND statType = "YDS"
                ORDER BY stat DESC
                LIMIT 5)
                ORDER BY total_yards DESC"""
    execute_query(sql)

def get_report_on_nfl_player(name):
    sql = f"""SELECT nfl_player.name, sum(targets), sum(receptions), sum(receptions), sum(touchdowns), max(longest_reception), ("NFL") AS League
             FROM nfl_receiving_per_game
             JOIN nfl_player
             ON nfl_receiving_per_game.player_id = nfl_player.player_id
             JOIN nfl_game
             ON nfl_game.game_id = nfl_receiving_per_game.game_id
             WHERE MATCH(name)
             AGAINST ('{name}')
             GROUP BY nfl_player.player_id"""
    execute_query(sql)

def get_surprising_draft_picks():
    sql =    """SELECT *
                FROM  college_player_stats
                JOIN college_player
                ON college_player.playerId = college_player_stats.playerId
                WHERE statType = "YDS"
                AND stat < 1000
                AND college_player_stats.playerId IN(
                                SELECT draft_pick_athlete.collegeAthleteId
                                FROM draft_pick_athlete
                                JOIN draft_pick
                                ON draft_pick_athlete.collegeAthleteId = draft_pick.collegeAthleteId
                                WHERE overall <= 50
                                )
                ORDER BY stat DESC"""
    execute_query(sql)


if __name__ == "__main__":
    get_the_top5_colleges_who_has_the_most_first_round_picks_in_the_draft()
    get_leading_receivers_in_the_last_two_seasons()
    most_yards_by_the_leading_reciver_in_each_conference()
    compare_nfl_and_colleges_by_receivers()
    get_report_on_nfl_player("michael")
    get_surprising_draft_picks()