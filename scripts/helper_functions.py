from datetime import datetime
import hashlib
import os
import polars as pl
import json

import plotly.express as px

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
        return pl.read_csv(source=abs_path,
                           has_header=True,
                           infer_schema=True)


def return_settings_data(abs_path: str) -> dict:
    if not os.path.isfile(abs_path):
        return None
    else:
        with open(abs_path) as json_file:
            return json.load(json_file)


def calculate_performance(df: pl.DataFrame, multipliers: dict) -> pl.DataFrame:
    performance_df = pl.DataFrame(data=[],
                                  schema={"gameid": pl.String,
                                          "date": pl.String,
                                          "team": pl.String,
                                          "position": pl.String,
                                          "playername": pl.String,
                                          "performance": pl.Float64})
    if df is None:
        return performance_df

    player_df = df.filter(pl.col("playername") != "team")

    for row in player_df.rows(named=True):
        if row["playername"] is None:
            continue
        performance_score = 0
        for key, value in multipliers["base"].items():
            performance_score += float(row[key]) * float(value)

        if row["position"] =="sup":
            for key, value in multipliers["extra"]["sup"].items():
                performance_score += float(row[key]) * float(value)
        if row["position"] =="jng":
            for key, value in multipliers["extra"]["jng"].items():
                performance_score += float(row[key]) * float(value)

        performance_score = round(performance_score, 2)
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


def calculate_fantasy_team_performance(event_data: pl.DataFrame,
                                       owner_fantasy_teams: pl.DataFrame,
                                       match_data: pl.DataFrame,
                                       multipliers_json: dict,
                                       modifiers_json: dict):
    event_matches_dict = event_matches_dictionary(event_data,
                                                  match_data)
    teams_score_df = None
    for team in owner_fantasy_teams.rows(named=True):

        #event_matches = event_matches_dict[team["eventname"]]

        if teams_score_df is None:
            teams_score_df = score_for_specific_team(team,
                                                     event_matches_dict,
                                                     multipliers_json,
                                                     modifiers_json)
        else:
            teams_score_df = teams_score_df.vstack(score_for_specific_team(team,
                                                                           event_matches_dict,
                                                                           multipliers_json,
                                                                           modifiers_json))

    return teams_score_df


def event_matches_dictionary(event_data: pl.DataFrame,
                             match_data: pl.DataFrame) -> dict:
    event_matches = dict()
    for event in event_data.rows(named=True):
        event_matches[event["name"]] = return_filtered_matches(match_data,
                                                               event["start_str"],
                                                               event["end_str"])
    return event_matches


def score_for_specific_team(team_dict: dict,
                            event_matches_dict: dict,
                            multipliers_json: dict,
                            modifiers_json: dict) -> pl.DataFrame:
    overall_score = 0.0
    event_matches = event_matches_dict[team_dict["eventname"]]
    score_dict = {"eventname": team_dict["eventname"],
                  "overall": 0.0,
                  "top": 0.0,
                  "jng": 0.0,
                  "mid": 0.0,
                  "bot": 0.0,
                  "sup": 0.0,
                  "modifier_diff": 0.0}

    if event_matches.count == 0:
        return score_dict
    else:
        positions = ["top", "jng", "mid", "bot", "sup"]

        for position in positions:
            player_matches = event_matches.filter(pl.col("playername") == team_dict[position])
            player_performance = calculate_performance(player_matches, multipliers_json)

            if team_dict["player"] is not None and team_dict["player"] == team_dict[position]:
                old_performance = player_performance
                player_performance = player_performance.with_columns((pl.col("performance") * check_for_modifier(team_dict["modifier"],
                                                                                                                 player_matches,
                                                                                                                 modifiers_json)).alias("performance"))

                score_dict[position] = player_performance["performance"].sum()
                score_dict["modifier_diff"] = round((player_performance["performance"].sum() - old_performance["performance"].sum()), 2)
                overall_score += player_performance["performance"].sum()
            else:
                score_dict[position] = player_performance["performance"].sum()
                overall_score += player_performance["performance"].sum()

        score_dict["overall"] = round(overall_score, 2)
        return pl.DataFrame(data=score_dict)


def check_for_modifier(modifier_type: str,
                       player_matches: pl.DataFrame,
                       modifier_dict: dict) -> pl.Series:
    if player_matches is None or player_matches.count == 0:
        return None

    match modifier_type:
        case "firstblood":
            mask = player_matches["firstblood"] == 1
        case "teemo":
            mask = player_matches["champion"] == "Teemo"
        case "deathless":
            mask = player_matches["deaths"] == 0
        case "triple+":
            mask = ((player_matches["triplekills"] > 0) |
                    (player_matches["quadrakills"] > 0) |
                    (player_matches["pentakills"] > 0))
        case "dmg 40k+":
            mask = player_matches["damagetochampions"] >= 40000
        case "dmg 100k+":
            mask = player_matches["damagetochampions"] >= 100000
        case "visionary(200+)":
            mask = player_matches["visionscore"] >= 200
        case "early loser(-1k gold@10)":
            mask = player_matches["golddiffat10"] <= -1000
        case "cs 350+":
            mask = player_matches["total cs"] >= 350

    mask = mask.map_elements(lambda x: modifier_dict[modifier_type][0] if x else modifier_dict[modifier_type][1],
                             skip_nulls=False)
    return mask


def return_combined_results_of_each_owner(team_owners: list,
                                          schedule_df,
                                          match_data_df,
                                          settings_json):
    combined_results = None
    for team_owner in team_owners:
        if combined_results is None:
            combined_results = return_event_selection(os.path.abspath(f"data/teams/{team_owner}_teams.csv"))

            combined_results = calculate_fantasy_team_performance(schedule_df,
                                                                  combined_results,
                                                                  match_data_df,
                                                                  settings_json["multipliers"],
                                                                  settings_json["modifiers"])
            if combined_results is None:
                continue
            combined_results = combined_results.with_columns(owner=pl.lit(team_owner))
        else:
            new_results = return_event_selection(os.path.abspath(f"data/teams/{team_owner}_teams.csv"))
            new_results = calculate_fantasy_team_performance(schedule_df,
                                                             new_results,
                                                             match_data_df,
                                                             settings_json["multipliers"],
                                                             settings_json["modifiers"])
            if new_results is None:
                return combined_results

            new_results = new_results.with_columns(owner=pl.lit(team_owner))
            combined_results = combined_results.vstack(new_results)

    return combined_results


def read_uploaded_file(file):
    return pl.read_csv(source=file,
                       has_header=True,
                       infer_schema=True,
                       skip_rows_after_header=100000)


def return_fantasy_teams_by_stage(data_folder_path: str):
    owners = pl.read_csv(f"{data_folder_path}/logins.csv")["name"].to_list()
    schedule_df = read_schedule(f"{data_folder_path}/schedule.csv")
    match_df = return_match_data(f"{data_folder_path}/match_data.csv")
    settings_json = return_settings_data(f"{data_folder_path}/settings.json")
    combined_results = return_combined_results_of_each_owner(owners,
                                                             schedule_df,
                                                             match_df,
                                                             settings_json)
    events = list(reversed(schedule_df["name"].to_list()))
    fantasy_teams = dict()
    for owner in owners:
        fantasy_teams[owner] = return_event_selection(f"{data_folder_path}/teams/{owner}_teams.csv")

    results_df = None
    for event in events:
        for owner in owners:
            if results_df is None:
                results_df = fantasy_teams[owner].filter(pl.col("eventname") == event)
                results_df = results_df.with_columns(owner=pl.lit(owner))

            else:
                new_row = fantasy_teams[owner].filter(pl.col("eventname") == event)
                new_row = new_row.with_columns(owner=pl.lit(owner))
                results_df = results_df.vstack(new_row)
            filtered_player_results = combined_results.filter((pl.col("eventname") == event) &
                                                              (pl.col("owner") == owner))
            if filtered_player_results.is_empty() == False:
                fpr = filtered_player_results.row(0, named=True)
                fpr_row = pl.DataFrame(data=[[str(fpr["overall"]),
                                              str(fpr["top"]),
                                              str(fpr["jng"]),
                                              str(fpr["mid"]),
                                              str(fpr["bot"]),
                                              str(fpr["sup"]),
                                              str("-"),
                                              str(fpr["modifier_diff"]),
                                              str(owner)]],
                                       schema=["eventname",
                                               "top",
                                               "jng",
                                               "mid",
                                               "bot",
                                               "sup",
                                               "modifier",
                                               "player",
                                               "owner"],
                                       strict=False)
                results_df = results_df.vstack(fpr_row)

    return results_df



