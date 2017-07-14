-- Table definitions for the tournament project.
--
-- Put your SQL 'create table' statements in this file; also 'create view'
-- statements if you choose to use it.
--
-- You can write comments in this file by starting them with two dashes, like
-- these lines here.
-- Check for and drop database if exists:
DROP DATABASE IF EXISTS tournament;

-- CREATE and CONNECT to tournament:
CREATE DATABASE tournament;
\c tournament;    

/*    Table of players.
    
    Columns:
        id: unique id of player (primary key)
        name: name of player
*/
CREATE TABLE players(id SERIAL PRIMARY KEY, 
                     name TEXT);


/*    Table of match results of all tournaments.

    Columns:
        match_id: unique match event (primary key)
        tournament_id: tournament id of given match
        winner_id: id of winning player
        loser_id: id of losing player
*/
CREATE TABLE matches(match_id SERIAL PRIMARY KEY,
                     winner_id INTEGER REFERENCES players(id), 
                     loser_id INTEGER REFERENCES players(id));


-- Views used for player_standings:

-- Find number of matches played per player:
CREATE
    OR REPLACE VIEW num_matches AS

SELECT players.id
    ,COALESCE(COUNT(players.id), 0) AS games_played
FROM players
    ,matches
WHERE (
        players.id = winner_id
        OR players.id = loser_id
        )
GROUP BY players.id
ORDER BY games_played DESC;


-- Find number of matches won per player:

SELECT players.id AS id
    ,players.NAME
    ,COUNT(subQuery.winner_id) AS wins
FROM players
LEFT JOIN (
    SELECT winner_id
    FROM matches
    ) AS subQuery ON players.id = subQuery.winner_id
GROUP BY players.id;


-- Find opponent match wins (games):
-- (subquery creates 2 columns of id & every opponent he has played)
CREATE
    OR REPLACE VIEW games AS

SELECT player_id
    ,CAST(SUM(wins) AS INT) AS opponent_wins
FROM games_won
    ,(
        SELECT winner_id AS player_id
            ,loser_id AS opponent
        FROM matches
        
        UNION
        
        SELECT loser_id AS player_id
            ,winner_id AS opponent
        FROM matches
        ORDER BY x ASC
        ) AS subQuery
WHERE id = opponent
GROUP BY player_id
ORDER BY player_id;
