import psycopg2

"""
    tournament.py -- implementation of a Swiss-system tournament.
"""


def connect(database_name="tournament"):
    """Connect to PostgreSQL database. Returns connection and cursor."""
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print "Error while trying to connect to database"


def deleteMatches():
    """Removes all the match records and from database."""

    db, cursor = connect()
    cursor.execute("""TRUNCATE matches;""")
    db.commit()
    db.close()


def deletePlayers():
    """Removes all the player as well as match records from database."""

    db, cursor = connect()
    cursor.execute("""TRUNCATE players CASCADE;""")
    db.commit()
    db.close()


def countPlayers(tournament_id=0):
    """Returns the number of players currently registered in selected
    tournament. If no arguments are given, then function returns the
    count of all registered players.

    Args:
        tournament_id: the id (integer) of the tournament.

    Returns:
        An integer of the number of registered players if tournament_id
        is not specified. If tournament_id is specified, returns an
        integer of the number of players in given tournament (who have
        played at least 1 game).
    """

    db, cursor = connect()

    if tournament_id == 0:
        cursor.execute("""SELECT COUNT(id) FROM players;""")
    else:
        cursor.execute("""SELECT CAST(COUNT(subQuery.X) AS INTEGER) FROM
                    (SELECT winner_id AS X
                     FROM matches WHERE matches.tournament_id = %s
                     UNION
                     SELECT loser_id AS X
                     FROM matches WHERE matches.tournament_id = %s)
                     AS subQuery;""", (tournament_id, tournament_id))

    output_ = cursor.fetchone()[0]
    db.close()
    return output_


def registerPlayer(name):
    """  Adds a player to the tournament database.

    Args:
        name: the player's full name (need not be unique).
    """

    db, cursor = connect()

    # add player to players table and return its id:
    query = "INSERT INTO players(name) VALUES(%s) RETURNING id;"
    param = (name,)
    cursor.execute(query, param)
    return_id = cursor.fetchone()[0]

    db.commit()
    db.close()
    return return_id


def playerStandings(tournament_id=0):
    """  Returns a list of the players & their win record, sorted by rank.

    The first entry in the returned list is the person in first place,
    or a player tied for first place if there is currently a tie. Due to
    the requirements of the test cases to return standings before any
    matches have been played, if tournament_id is not specified (i.e.
    tournament_id = 0), the function returns all registered players
    regardless of what tournament they are participating in; otherwise,
    the function returns standings of only the players that have played
    at least 1 game in given tournament_id.

    Args:
        tournament_id: specifies the tournament from which we want the
        results - defaults to 0.

    Returns:
        A list of tuples, each containing (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played.
    """

    db, cursor = connect()

    #  check how many matches have been played:
    query = """SELECT COUNT(match_id) FROM matches
               WHERE matches.tournament_id = %s;"""
    param = (tournament_id,)
    cursor.execute(query, param)
    total_matches = cursor.fetchone()[0]

    #  if no matches have been played, return registered players:
    if total_matches == 0:
        cursor.execute("""SELECT id, name, 0, 0 FROM players;""")
        output_ = cursor.fetchall()
        db.close()
    return output_

    #  Joins tables and returns standings -- sorted by wins, omw:
    #  if tournament_id is specified, returns only players that have
    #  played in that specific tournament:
    if tournament_id != 0:
        tournament_restriction = 'AND games_played > 0'
    else:
        tournament_restriction = ''
    #  (subquery combines wins-count data with opponent wins (omw) &
    #  select number of games played)
    cursor.execute("""SELECT subQuery.id, subQuery.name, wins,
             CAST(games_played AS INTEGER)
             FROM num_matches,
                  (SELECT games_won.id AS id, name,
                   CAST(wins AS INTEGER),
                   ON id = x)
                   AS subQuery
             WHERE (subQuery.id = num_matches.id) %s
             ORDER BY wins DESC, opponent_wins DESC,
             games_played ASC;""" % tournament_restriction)

        output_ = cursor.fetchall()
        db.close()
    return output_


def ReportMatch(winner, loser, tournament_id=0):
    """  Records the outcome of a single match between two players.

    Args:
        winner:  the id number of the player who won
        loser:  the id number of the player who lost
        tournament_id:  the match's tournament id
    """

    db, cursor = connect()

    #  Add players into matches table:
    query = "INSERT INTO matches (winner_id, loser_id, tournament_id) VALUES(%s, %s, %s);"
    param = (winner, loser, tournament_id)
    cursor.execute(query, param)

    db.commit()
    db.close()


def swissPairings(tournament_id=0):
    """  Returns a list of pairs of players for the next round of a match.

    Assuming that there are an even number of players registered, each
    player appears exactly once in the pairings.  Each player is paired
    with another player with an equal or nearly-equal win record, that
    is, a player adjacent to him in the standings.

    Returns:
        A list of tuples, each containing (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """

    standings = playerStandings(tournament_id)

    pairings = []
    iii = 1
    while iii < len(standings):
        tup1 = standings[iii-1]
        tup2 = standings[iii]

        pairings.append(tuple((tup1[0], tup1[1], tup2[0], tup2[1])))
        iii += 2

    return pairings


if __name__ == "__main__":
    def test_omw():
        """  Tests the congruity of Opponent Match Wins

        If players have same number of wins, omw ranks players by opponents'
        wins. This test case checks for omw congruity.
        """

        #  clear data:
        deleteMatches()
        deletePlayers()

        #  register players:
        players = ["Attila", "Bleda", "Rugila", "Ernak", "Nimrod",
                   "Temujin", "Subutai", "Ogedei", "Toregene", "Kublai"]
        [registerPlayer(x) for x in players]

        standings = playerStandings()
        [id0, id1, id2, id3, id4,
            id5, id6, id7, id8, id9] = [row[0] for row in standings]

        #  report matches:
        ReportMatch(id0, id9)
        ReportMatch(id1, id8)
        ReportMatch(id2, id7)
        ReportMatch(id3, id6)
        ReportMatch(id4, id5)
        ReportMatch(id0, id1)
        ReportMatch(id2, id3)
        ReportMatch(id4, id5)
        ReportMatch(id6, id7)
        ReportMatch(id8, id9)
        ReportMatch(id0, id2)
        ReportMatch(id1, id3)
        ReportMatch(id6, id8)
        ReportMatch(id6, id2)

        #  check results (first 8 players):
        standings = playerStandings()
        correct_results = frozenset([id0, id6, id2, id1, id4, id3, id8, id7])
        user_results = frozenset([row[0] for row in standings[:8]])

        if correct_results != user_results:
            print "Expected:", correct_results
            print "Recieved:", user_results
            raise ValueError("Incorrect player rank!")

        print "OMW is implemented correctly"

        #  clear data:
        deleteMatches()
        deletePlayers()