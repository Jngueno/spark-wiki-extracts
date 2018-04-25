package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    standings.createOrReplaceTempView("leagueStands")
    standings.sqlContext.sql("SELECT season, league, MEAN(goalsFor) FROM leagueStands GROUP BY season, league").show()

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    standings.groupBy(standings.col("league")).agg(
      min(mean(standings.col("position")))
    ).show()

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    standings
      .filter("position = 1")
      .groupBy(standings.col("league"), standings.col("team"))
      .mean("points")
      .show()

    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val decadeScala : Int => String = _.toString.dropRight(1) + "X"

    val decade = udf(decadeScala)
    session.udf.register("decade", (season : Int) => (season / 10).toString + "X") // Usable for sql usage

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")
    standings
      .filter("position = 1 or position = 10")
      .withColumn("decade", decade('season))
      .groupBy(standings.col("league"), standings.col("decade"))
      .agg(
        mean(max("position") - min("position"))
      )
  }
}
