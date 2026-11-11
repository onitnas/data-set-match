
from pyspark.sql import functions as F


BEST_OF_5_TOURNAMENTS = ["1236", "2155", "2053", "1217"]


def score_number(column, index):
    value = F.get(F.split(column, "-"), index)
    return F.trim(value).try_cast("double")


def build_snapshot_features(df, tournaments, rankings):

    match = F.col("data.match")

    # Ultimo set presente nell'array scores
    scores = match["scores"]
    current_score = F.get(scores, F.size(scores) - 1)

    # Punteggio del game corrente
    points = F.split(match["event_game_result"], "-")

    score_map = F.create_map(
        F.lit("0"), F.lit(0.0),
        F.lit("15"), F.lit(1.0),
        F.lit("30"), F.lit(2.0),
        F.lit("40"), F.lit(3.0),
        F.lit("A"), F.lit(4.0)
    )

    result = (
        df
        .withColumn(
            "best_of",
            F.when(
                match["tournament_key"].cast("string").isin(
                    BEST_OF_5_TOURNAMENTS
                ),
                5.0
            ).otherwise(3.0)
        )
        .withColumn(
            "sets_a",
            score_number(match["event_final_result"], 0)
        )
        .withColumn(
            "sets_b",
            score_number(match["event_final_result"], 1)
        )
        .withColumn(
            "player_a_is_serving",
            F.when(match["event_serve"] == "First Player", 1.0)
             .when(match["event_serve"] == "Second Player", 0.0)
        )
    )

    result = (
        result
        .withColumn(
            "games_a",
            current_score["score_first"].try_cast("double")
        )
        .withColumn(
            "games_b",
            current_score["score_second"].try_cast("double")
        )
    )
    result = result.withColumn(
        "is_tiebreak",
        F.when(
            (F.col("games_a") == 6) &
            (F.col("games_b") == 6),
            1.0
        )
        .when(
            F.col("games_a").isNull() |
            F.col("games_b").isNull(),
            F.lit(None).cast("double")
        )
        .otherwise(0.0)
    )

    result = result.withColumn(
        "tiebreak_target",
        F.when(
            F.col("is_tiebreak") == 1,
            F.when(
                (F.col("best_of") == 5) &
                (F.col("sets_a") == 2) &
                (F.col("sets_b") == 2),
                10.0
            ).otherwise(7.0)
        )
        .when(F.col("is_tiebreak") == 0, 0.0)
    )

    result = (
        result
        .withColumn(
            "regular_score_a",
            F.when(
                F.col("is_tiebreak") == 0,
                score_map[F.trim(F.get(points, 0))]
            ).when(F.col("is_tiebreak") == 1, 0.0)
        )
        .withColumn(
            "regular_score_b",
            F.when(
                F.col("is_tiebreak") == 0,
                score_map[F.trim(F.get(points, 1))]
            ).when(F.col("is_tiebreak") == 1, 0.0)
        )
    )

    result = (
        result
        .withColumn(
            "tiebreak_score_a",
            F.when(
                F.col("is_tiebreak") == 1,
                score_number(match["event_game_result"], 0)
            ).when(F.col("is_tiebreak") == 0, 0.0)
        )
        .withColumn(
            "tiebreak_score_b",
            F.when(
                F.col("is_tiebreak") == 1,
                score_number(match["event_game_result"], 1)
            ).when(F.col("is_tiebreak") == 0, 0.0)
        )
    )

    result = result.join(
        tournaments,
        match["tournament_key"].cast("string") ==
        F.col("config_tournament_key"),
        how="left"
    ).drop("config_tournament_key")


    result = result.withColumn(
        "ranking_year",
        F.year(match["event_date"].try_cast("date")) - 1
    )

    rank_a = rankings.select(
        F.col("player_id").alias("a_id"),
        F.col("season").alias("a_season"),
        F.col("rank").alias("player_a_rank")
    )

    result = result.join(
        rank_a,
        (match["first_player_key"].cast("string") == F.col("a_id")) &
        (F.col("ranking_year") == F.col("a_season")),
        how="left"
    ).drop("a_id", "a_season")

    rank_b = rankings.select(
        F.col("player_id").alias("b_id"),
        F.col("season").alias("b_season"),
        F.col("rank").alias("player_b_rank")
    )

    result = result.join(
        rank_b,
        (match["second_player_key"].cast("string") == F.col("b_id")) &
        (F.col("ranking_year") == F.col("b_season")),
        how="left"
    ).drop("b_id", "b_season", "ranking_year")


    result = (
        result
        .withColumn(
            "player_a_rank",
            F.coalesce(
                F.col("player_a_rank"), F.lit(0)
            ).cast("double")
        )
        .withColumn(
            "player_b_rank",
            F.coalesce(
                F.col("player_b_rank"), F.lit(0)
            ).cast("double")
        )
        .withColumn(
            "player_a_rank_missing",
            F.when(
                F.col("player_a_rank") == 0, 1.0
            ).otherwise(0.0)
        )
        .withColumn(
            "player_b_rank_missing",
            F.when(
                F.col("player_b_rank") == 0, 1.0
            ).otherwise(0.0)
        )
    )

    return result
