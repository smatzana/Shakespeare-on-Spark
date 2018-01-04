package com.matzanas.shakespeare_on_Spark

import com.matzanas.shakespeare_on_Spark.ShakespeareOnSpark.shakespeareSparkAccumulator
import org.apache.spark.rdd.RDD

sealed case class PhraseLength(s: String, count: Int) {
  override def toString = s"(${count} characters) ${s}"
}

object RDDOps {

  implicit class ImplicitRdd(rdd: RDD[String]) {

    def keepOnlySpokenLines = rdd.filter(!_.endsWith(">"))

    def removeWhiteSpaceAndMarkup = rdd.map(SparkUtilities.cleanseInputLine)

    def removePunctuation = rdd.map(SparkUtilities.removeLinePunctuation)

    def extractWords = rdd.flatMap(s => {
      val words = s.split("\\s+")
      shakespeareSparkAccumulator.add(ShakespeareMetrics(0, words.length))
      words
    })

    /*
      Perform some rudimentary normalization, lowercase all words, replace
      "noun's" with original "noun"

      This is very basic and did not want to overdo it, but wanted to put a placeholder here
     */
    def normalizeWords = rdd.map(_.toLowerCase.replaceAll("(\\p{Alpha})'s", "$1"))

    def sortByWordCountDesc(minLen: Int = 4) = rdd
      .filter(_.length >= minLen)
      .map(s => (s, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    /*
     Use MapPartition for longest words, as it will run
     more efficiently than a simple map
     */
    def sortByWordLengthDescMapPartition = rdd
      .mapPartitions(SparkUtilities.pullTopFiveFromIterator)
      .sortBy(_._1, false)

    def splitPhrasesAndSortDesc = rdd
      .map(SparkUtilities.normalizeQuotes)
      .flatMap(_.split("[.?;!:]"))
      .map(s => PhraseLength(s.trim, s.length))
      .distinct()
      .sortBy(_.count, false)
  }

}
