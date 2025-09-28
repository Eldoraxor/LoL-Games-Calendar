from mwrogue.esports_client import EsportsClient
import pandas as pd
#from postgre_con import postgres_update_data
#from download_json import register_new_JSON

site = EsportsClient("lol")

def get_tournaments(tournament_names : str):
    site = EsportsClient("lol")
    tournaments = site.cargo_client.query(
        tables="Tournaments = T",
        where=f"T.StandardName = {tournament_names}",
        fields="T.Name, T.OverviewPage, T.DateStart, T.League, T.Region, T.Prizepool, T.Country, T.Rulebook, T.EventType, T.StandardName, T.Split, T.SplitNumber, T.Year, T.LeagueIconKey, T.IsOfficial"
    )
    tournaments_df = pd.DataFrame(tournaments)
    tournaments_df.drop(["DateStart__precision"], axis=1, inplace=True)
    return tournaments_df

def get_scoreboard_games(tournament_names : str):
    site = EsportsClient("lol")
    games = site.cargo_client.query(
        tables="ScoreboardGames = SG",
        where= f"SG.Tournament IN {tournament_names}",
        fields="RiotPlatformGameId, Tournament, DateTime_UTC, Winner, Gamelength , Gamelength_Number, Patch, Gamename, MatchId, GameId, OverviewPage"
    )
    games_df = pd.DataFrame(games)
    games_df["Gamelength"] = "00:" + games_df["Gamelength"]
    games_df.drop(["DateTime UTC__precision"], axis=1, inplace=True)
    return games_df

def get_teams(team_names: str):
    site = EsportsClient("lol")
    teams = site.cargo_client.query(
        tables="Teams=T",
        where=f"T.OverviewPage IN {team_names}",
        fields="Name, OverviewPage, Short, Location, Region, Image, IsDisbanded, RenamedTo"
    )
    teams_df = pd.DataFrame(teams)
    return teams_df

def get_scoreboard_teams(tournament_names : str):
    site = EsportsClient("lol")
    team_games = site.cargo_client.query(
        tables="ScoreboardTeams = ST, ScoreboardGames = SG",
        join_on="ST.GameId = SG.GameId",
        where=f"SG.Tournament IN {tournament_names}",
        fields="SG.GameId, ST.Team, ST.Side, ST.IsWinner, ST.Score, ST.Bans, ST.Dragons, ST.Barons, ST.Towers, ST.Gold, ST.Kills, ST.RiftHeralds, ST.VoidGrubs, ST.Inhibitors"
    )
    team_games_df = pd.DataFrame(team_games)
    team_games_df["GameTeamId"] = team_games_df["GameId"] + "_" + team_games_df["Team"]
    team_games_df["Team"] = team_games_df["Team"].apply(lambda x: x[0].upper() + x[1:] if isinstance(x, str) and x else x)
    return team_games_df

def get_players(player_names: str):
    site = EsportsClient("lol")
    players = site.cargo_client.query(
        tables="Players=P",
        where=f"P.Player IN {player_names}",
        fields="ID, Player, Name, NativeName, Country, Nationality, NationalityPrimary, Birthdate, Role, Residency, Lolpros, IsRetired, OverviewPage"
    )
    players_df = pd.DataFrame(players)
    #players_df.drop(["Birthdate__precision"], axis=1, inplace=True)
    return players_df

def get_scoreboard_players(tournament_names: str):
    site = EsportsClient("lol")
    player_games = site.cargo_client.query(
        tables="ScoreboardPlayers = SP, ScoreboardGames = SG",
        join_on="SP.GameId = SG.GameId",
        where=f"SG.Tournament IN {tournament_names}",
        fields="SG.GameId, SP.Name, SP.Link, SP.Champion, SP.Kills, SP.Deaths, SP.Assists, SP.SummonerSpells, SP.Gold, SP.CS, SP.Role, SP.Role_Number, SP.DamageToChampions, SP.VisionScore, SP.Items, SP.Trinket, SP.KeystoneMastery, SP.KeystoneRune, SP.PrimaryTree, SP.SecondaryTree, SP.Runes, SP.Team"
    )
    player_games_df = pd.DataFrame(player_games)
    player_games_df["GameTeamId"] = player_games_df["GameId"] + "_" + player_games_df["Team"]
    player_games_df["GameRoleIdVs"] = player_games_df["GameTeamId"] + "_" + player_games_df["Role"]
    player_games_df.drop(["Team", "GameId"], axis=1, inplace=True)
    return player_games_df