import datetime
import hashlib
import os
import polars as pl


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
                                           "end_str": pl.String})
        schedule_df.write_csv(file=abs_path)
    else:
        schedule_df = pl.read_csv(source=abs_path,
                                  schema={"stage": pl.String,
                                          "name": pl.String,
                                          "start_str": pl.String,
                                          "end_str": pl.String})

    return schedule_df


def read_players(abs_path: str):
    if not os.path.isfile(abs_path):
        players_df = pl.DataFrame(data=[],
                                  schema={"team": pl.String,
                                          "top": pl.String,
                                          "jungle": pl.String,
                                          "mid": pl.String,
                                          "adc": pl.String,
                                          "support": pl.String,
                                          "eliminated": pl.Boolean})
        players_df.write_csv(file=abs_path)
    else:
        players_df = pl.read_csv(source=abs_path,
                                 schema={"team": pl.String,
                                         "top": pl.String,
                                         "jungle": pl.String,
                                         "mid": pl.String,
                                         "adc": pl.String,
                                         "support": pl.String,
                                         "eliminated": pl.Boolean})
    return players_df


def return_match_data(abs_path: str) -> pl.DataFrame:
    if not os.path.isfile(abs_path):
        return None
    else:
        return pl.read_csv(source=abs_path)
