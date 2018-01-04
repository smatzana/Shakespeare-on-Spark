package com.matzanas.shakespeare_on_Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ShakespeareOnSpark extends App {

  val level = Level.WARN
  Logger.getLogger("org").setLevel(level)
  Logger.getLogger("akka").setLevel(level)

  if (args.length == 0) {
    println("Missing input path")
    System.exit(-1)
  }

  val filePath = args(0)

  val sc = new SparkContext(new SparkConf().setAppName("ShakespeareOnSpark").setMaster("local[*]"))
  var shakespeareSparkAccumulator = new ShakespeareSparkAccumulator
  sc.register(shakespeareSparkAccumulator, "ShakespeareOnSparkAccumulator")

  import SparkJobs._

  // Split input on multi-lines separated by Stage/actor direction opener : <
  implicit val shakespeareWorksRdd: RDD[String] = readShakespeareWorks(sc, filePath)

  println("Top 5 Words (no min length):")
  topWords(0).take(5).foreach(println)

  println("\nTop 5 Words (min length 4):")
  topWords().take(5).foreach(println)

  println("\nTop 5 Longest Words:")
  topLongestWords.take(5).foreach(println)

  println("\nTop 5 Longest Phrases:\n----------------")
  topPhrases.take(5).foreach(p => println(s"${p}\n----------------"))

  // Some stats
  println(s"\nStats: ${shakespeareSparkAccumulator.value}")
  sc.stop()
}
