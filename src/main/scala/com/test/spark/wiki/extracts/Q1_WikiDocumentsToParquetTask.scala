package com.test.spark.wiki.extracts

import java.io.FileReader
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  import session.implicits._
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    // TODO Q1 Transformer cette seq en dataset
    getLeagues
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatageimport org.jsoup.Jsoup

            val leagueStandings = Seq(LeagueStanding)
            val classementPage = Jsoup.connect(url).get
            val tableClassements = classementPage.select("h3:has(> #Classement) ~ table.wikitable.gauche tbody tr")
            for (e <- tableClassements) {
              if(e.child(0).tagName() != "th") {
                val position = e.child(0).text().toInt
                val team = e.child(1).text()
                val points = e.child(2).text().toInt
                val played = e.child(3).text().toInt
                val won = e.child(4).text().toInt
                val drawn = e.child(5).text().toInt
                val lost = e.child(6).text().toInt
                val goalsFor = e.child(7).text().toInt
                val goalsAgainst = e.child(8).text().toInt
                val goalsDifference = e.child(9).text().toInt
                logger.debug(league, team)
                val ls = LeagueStanding(
                  league, season, position, team, points, played, won, drawn, lost, goalsFor, goalsAgainst, goalsDifference
                )
                leagueStandings :+ ls
              }
            }
            leagueStandings
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              // N'ayant travailler que sur la Google Cloud Platform je ne maîtrise pas les environnements AWS
              // Ils seront stockes /mnt/var/log/spark/*
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .splitAt(10)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    // Organiser en colonne
    // Consomme moins d'espace
    // Similaire à une base de données (sauvegarde en table avec colonne nommée)
    // Récupération d'une colonne spécifique facile

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    // Don't know
    // J'assumerai que vu qu'une séquence est une sorte de liste indexés cela permet de faciliter la conversion
  }

  private def getLeagues: Seq[LeagueInput] = {
    val inputStream = new FileReader("../resources/leagues.yaml")
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
