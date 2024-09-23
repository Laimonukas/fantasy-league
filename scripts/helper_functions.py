from datetime import datetime
import hashlib
import os
import polars as pl
import json

def return_hash(x: object) -> str:
    hash_object = hashlib.md5(str(x).encode())
    return hash_object.hexdigest()


def read_login(abs_path: str) -> pl.DataFrame:
    if os.path.isfile(abs_path):
        login_df = pl.read_csv(abs_path,
                               schema={"id": pl.Int64,
                                       "name": pl.String,
                                       "password": pl.String,
                                       "admin": pl.Boolean})
    else:
        login_df = pl.DataFrame(data=[[0, "admin", return_hash(123), True]],
                                schema={"id": pl.Int64,
                                        "name": pl.String,
                                        "password": pl.String,
                                        "admin": pl.Boolean})
        login_df.write_csv(file=abs_path)
    return login_df


def read_schedule(abs_path: str):
    if not os.path.isfile(abs_path):
        schedule_df = pl.DataFrame(data=[],
                                   schema={"stage": pl.String,
                                           "name": pl.String,
                                           "start_str": pl.String,
                                           "end_str": pl.String,
                                           "locked": pl.Boolean})
        schedule_df.write_csv(file=abs_path)
    else:
        schedule_df = pl.read_csv(source=abs_path,
                                  schema={"stage": pl.String,
                                          "name": pl.String,
                                          "start_str": pl.String,
                                          "end_str": pl.String,
                                          "locked": pl.Boolean})

    return schedule_df


def read_players(abs_path: str):
    if not os.path.isfile(abs_path):
        players_df = pl.DataFrame(data=[],
                                  schema={"team": pl.String,
                                          "top": pl.String,
                                          "jng": pl.String,
                                          "mid": pl.String,
                                          "bot": pl.String,
                                          "sup": pl.String,
                                          "eliminated": pl.Boolean})
        players_df.write_csv(file=abs_path)
    else:
        players_df = pl.read_csv(source=abs_path,
                                 schema={"team": pl.String,
                                         "top": pl.String,
                                         "jng": pl.String,
                                         "mid": pl.String,
                                         "bot": pl.String,
                                         "sup": pl.String,
                                         "eliminated": pl.Boolean})
    return players_df


def return_match_data(abs_path: str) -> pl.DataFrame:
    if not os.path.isfile(abs_path):
        return None
    else:
        return pl.read_csv(source=abs_path)


def return_settings_data(abs_path: str) -> dict:
    if not os.path.isfile(abs_path):
        return None
    else:
        with open(abs_path) as json_file:
            return json.load(json_file)


def calculate_performance(df: pl.DataFrame, multipliers: dict) -> pl.DataFrame:
    player_df = df.filter(pl.col("playername") is not None)

    performance_df = pl.DataFrame(data=[],
                                  schema={"gameid": pl.String,
                                          "date": pl.String,
                                          "team": pl.String,
                                          "position": pl.String,
                                          "playername": pl.String,
                                          "performance": pl.Float64})

    for row in player_df.rows(named=True):
        if row["playername"] is None:
            continue
        performance_score = 0
        for key, value in multipliers["base"].items():
            performance_score += row[key] * value
        performance_score = round(performance_score, 2)

        if row["position"] =="sup":
            for key, value in multipliers["extra"]["sup"].items():
                performance_score += row[key] * value
        if row["position"] =="jng":
            for key, value in multipliers["extra"]["jng"].items():
                performance_score += row[key] * value

        new_row = pl.DataFrame(data=[[row["gameid"],
                                      row["date"],
                                      row["teamname"],
                                      row["position"],
                                      row["playername"],
                                      performance_score]],
                               schema={"gameid": pl.String,
                                       "date": pl.String,
                                       "team": pl.String,
                                       "position": pl.String,
                                       "playername": pl.String,
                                       "performance": pl.Float64})
        performance_df = performance_df.vstack(new_row)

    return performance_df


def return_player_pricing(abs_path: str,
                          player_df: pl.DataFrame):
    if player_df is not None:
        if os.path.isfile(abs_path):
            pricing_df = pl.read_csv(source=abs_path,
                                     schema={"team": pl.String,
                                             "position": pl.String,
                                             "playername": pl.String,
                                             "playercost": pl.Int8,
                                             "suggestedplayercost": pl.Int8})
            return pricing_df
        else:
            return remake_player_pricing(abs_path, player_df)
    else:
        return None


def remake_player_pricing(abs_path: str,
                          player_df: pl.DataFrame,
                          performance_df: pl.DataFrame = None):
    player_df = player_df.filter(pl.col("eliminated") == False)

    positions = ["top", "jng", "mid", "bot", "sup"]
    pricing_df = pl.DataFrame(data=[],
                              schema={"team": pl.String,
                                      "position": pl.String,
                                      "playername": pl.String,
                                      "playercost": pl.Int8,
                                      "suggestedplayercost": pl.Int8})
    for position in positions:
        partial_df = player_df["team", position]
        for row in partial_df.rows(named=True):
            if performance_df is None:
                suggested_cost = 5
            else:
                suggested_cost = suggest_cost_for_player(performance_df,
                                                         row[position],
                                                         partial_df[position].unique().to_list())

            new_entry_df = pl.DataFrame(data=[[row["team"],
                                               position,
                                               row[position],
                                               5,
                                               suggested_cost]],
                                        schema={"team": pl.String,
                                                "position": pl.String,
                                                "playername": pl.String,
                                                "playercost": pl.Int8,
                                                "suggestedplayercost": pl.Int8})
            pricing_df = pricing_df.vstack(new_entry_df)
    pricing_df.write_csv(abs_path)
    return pricing_df


def return_avg_performance(abs_path: str,
                           performance_df: pl.DataFrame):
    if performance_df is None:
        return None
    elif os.path.isfile(abs_path):
        avg_df = pl.read_csv(source=abs_path,
                             schema={"position": pl.String,
                                     "meanscore": pl.Float64,
                                     "minscore": pl.Float64,
                                     "maxscore": pl.Float64})
        return avg_df
    else:
        return recalculate_avg_performance(abs_path, performance_df)


def recalculate_avg_performance(abs_path,
                                performance_df):
    avg_df = pl.DataFrame(data=[],
                          schema={"position": pl.String,
                                  "meanscore": pl.Float64,
                                  "minscore": pl.Float64,
                                  "maxscore": pl.Float64})
    positions = ["top", "jng", "mid", "bot", "sup"]
    for position in positions:
        partial_df = performance_df.filter(pl.col("position") == position)
        position_row = pl.DataFrame(data=[[position,
                                           partial_df["performance"].mean(),
                                           partial_df["performance"].min(),
                                           partial_df["performance"].max()]],
                                    schema={"position": pl.String,
                                            "meanscore": pl.Float64,
                                            "minscore": pl.Float64,
                                            "maxscore": pl.Float64})
        avg_df = avg_df.vstack(position_row)
    avg_df.write_csv(abs_path)
    return avg_df


def suggest_cost_for_player(performance_df: pl.DataFrame,
                            player_name: str,
                            players_list: list):

    player_performance = performance_df.filter(pl.col("playername") == player_name)
    if player_performance.is_empty():
        return 5
    else:

        player_position = player_performance["position"][0]
        position_performance_df = performance_df.filter(pl.col("position") == player_position)
        if len(players_list) > 0:
            position_performance_df = position_performance_df.filter(pl.col("playername").is_in(players_list))
        position_performance_df = position_performance_df.group_by(by="playername").agg(pl.col("performance").mean())
        position_performance_df = position_performance_df.with_columns(new_column=position_performance_df["performance"]
                                                                       .qcut(5,
                                                                             labels=["1","2","3","4","5"],
                                                                             allow_duplicates=True))

        return int(position_performance_df.filter(pl.col("by") == player_name)["new_column"][0])


def return_filtered_matches(match_df: pl.DataFrame,
                            start_date: str,
                            end_date: str) -> pl.DataFrame:
    filter_df = match_df.filter((pl.col("date").str.to_datetime() < datetime.strptime(end_date, "%Y-%m-%d")) &
                                (pl.col("date").str.to_datetime() > datetime.strptime(start_date, "%Y-%m-%d")))
    return filter_df


def return_event_selection(abs_path: str) -> pl.DataFrame:
    if os.path.isfile(abs_path):
        return pl.read_csv(source=abs_path,
                           schema={"eventname": pl.String,
                                   "top": pl.String,
                                   "jng": pl.String,
                                   "mid": pl.String,
                                   "bot": pl.String,
                                   "sup": pl.String,
                                   "modifier": pl.String,
                                   "player": pl.String})
    else:
        new_df = pl.DataFrame(data=[],
                              schema={"eventname": pl.String,
                                      "top": pl.String,
                                      "jng": pl.String,
                                      "mid": pl.String,
                                      "bot": pl.String,
                                      "sup": pl.String,
                                      "modifier": pl.String,
                                      "player": pl.String})
        new_df.write_csv(abs_path)
        return new_df


def stringify_player_costs(player_cost_df: pl.DataFrame) -> list:
    return [f"{row['playername']} ({row['playercost']})" for row in player_cost_df.rows(named=True)]

def return_only_team_owners(login_abs_path: str) -> list:
    return read_login(login_abs_path)["name"].to_list()