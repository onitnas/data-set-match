
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


BEST_OF_5_TOURNAMENTS = ["1236", "2155", "2053", "1217"]

def score_number(column, index):
    part = F.get(F.split(column, "-"), index)
    return F.trim(part).try_cast("double")


def build_snapshot_features(df, tournaments, rankings):
    match = F.col("data.match")
    sets = F.split(match["event_final_result"], "-")
    points = F.split(match["event_game_result"], "-")
    
    score_map = F.create_map(
        F.lit("0"), F.lit(0.0),
        F.lit("15"), F.lit(1.0),
        F.lit("30"), F.lit(2.0),
        F.lit("40"), F.lit(3.0),
        F.lit("A"), F.lit(4.0)
    )
    
    feature = (
        df
        .withColumn(
            "best_of",
            F.when(
                match["tournament_key"].isin(BEST_OF_5_TOURNAMENTS), 5
            ).otherwise(3).cast("double")
        )
        .withColumn("sets_a", score_number(F.col("data.match.event_final_result"), 0))
        .withColumn("sets_b", score_number(F.col("data.match.event_final_result"), 1))
        .withColumn(
            "player_a_is_serving",
            F.when(match["event_serve"] == "First Player", 1.0)
             .when(match["event_serve"] == "Second Player", 0.0)
             .otherwise(F.lit(None).cast("double"))
        )
        .withColumn("regular_score_a",score_map[F.get(points, 0)])
        .withColumn("regular_score_b",score_map[F.get(points, 1)])
    )
    
    result = (
        feature

        # Game appartenenti al set corrente
        .withColumn(
            "current_games",
            F.expr("""
                filter(
                    data.match.pointbypoint,
                    g -> g.set_number = data.match.event_status
                )
            """)
        )

        # Numero del game corrente
        .withColumn(
            "current_game",
            F.expr("""
                array_max(
                    transform(
                        current_games,
                        g -> try_cast(g.number_game AS INT)
                    )
                )
            """)
        )

        # Ultimo game con score disponibile
        .withColumn(
            "last_completed",
            F.expr("""
                array_max(
                    transform(
                        filter(current_games, g -> g.score IS NOT NULL),
                        g -> named_struct(
                            'number', try_cast(g.number_game AS INT),
                            'score', g.score
                        )
                    )
                )
            """)
        )

        # Punteggio dei game
        .withColumn(
            "last_score",
            F.when(
                (F.col("current_game") == 1) &
                F.col("last_completed.score").isNull(),
                F.lit("0 - 0")
            ).otherwise(F.col("last_completed.score"))
        )

        .withColumn("games_a",score_number(F.col("last_score"), 0))
        
        .withColumn("games_b",score_number(F.col("last_score"), 1))

        .drop("current_games", "current_game", "last_completed", "last_score")
    )
    
    result = result.withColumn("is_tiebreak", F.when(
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
            "tiebreak_score_a",
            F.when(
                F.col("is_tiebreak") == 1,
                score_number(match["event_game_result"], 0)            )
            .when(F.col("is_tiebreak") == 0, 0.0)
        )
        .withColumn(
            "tiebreak_score_b",
            F.when(
                F.col("is_tiebreak") == 1,
                score_number(match["event_game_result"], 1)
            )
            .when(F.col("is_tiebreak") == 0, 0.0)
        )
    )
    
    result = (
        result
        .withColumn(
            "regular_score_a",
            F.when(F.col("is_tiebreak") == 1, 0.0)
            .when(F.col("is_tiebreak") == 0, F.col("regular_score_a"))
        )
        .withColumn(
            "regular_score_b",
            F.when(F.col("is_tiebreak") == 1, 0.0)
            .when(F.col("is_tiebreak") == 0, F.col("regular_score_b"))
        )
    )
    
    result = result.join(
        tournaments,
        F.col("data.match.tournament_key").cast("string") ==
        F.col("config_tournament_key"),
        how="left"
    ).drop("config_tournament_key")
    
    # Anno del ranking: stagione precedente
    result = result.withColumn(
        "ranking_year",
        F.year(match["event_date"].try_cast("date")) - 1
    )

    # Ranking giocatore A
    rank_a = rankings.select(
        F.col("player_id").alias("a_id"),
        F.col("season").alias("a_season"),
        F.col("rank").alias("player_a_rank")
    )

    result = result.join(
        rank_a,
        (F.col("data.match.first_player_key").cast("string") == F.col("a_id")) &
        (F.col("ranking_year") == F.col("a_season")),
        how="left"
    ).drop("a_id", "a_season")

    # Ranking giocatore B
    rank_b = rankings.select(
        F.col("player_id").alias("b_id"),
        F.col("season").alias("b_season"),
        F.col("rank").alias("player_b_rank")
    )

    result = result.join(
        rank_b,
        (F.col("data.match.second_player_key").cast("string") == F.col("b_id")) &
        (F.col("ranking_year") == F.col("b_season")),
        how="left"
    ).drop("b_id", "b_season", "ranking_year")

    # Ranking mancante: 0, come nel training
    result = (
        result
        .withColumn(
            "player_a_rank",
            F.coalesce(F.col("player_a_rank"), F.lit(0)).cast("double")
        )
        .withColumn(
            "player_b_rank",
            F.coalesce(F.col("player_b_rank"), F.lit(0)).cast("double")
        )
        .withColumn(
            "player_a_rank_missing",
            F.when(F.col("player_a_rank") == 0, 1.0).otherwise(0.0)
        )
        .withColumn(
            "player_b_rank_missing",
            F.when(F.col("player_b_rank") == 0, 1.0).otherwise(0.0)
        )
    )
        
    return result