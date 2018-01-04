package com.matzanas.shakespeare_on_Spark

import com.matzanas.shakespeare_on_Spark.ShakespeareOnSpark.shakespeareSparkAccumulator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkJobs {

  import RDDOps._

  def readShakespeareWorks(sc: SparkContext, path: String) = {
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", "<")
    sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(s => {
        shakespeareSparkAccumulator.add(ShakespeareMetrics(1, 0))
        SparkUtilities.readAndScrubUTF16(s)
      })
  }

  /*
   Filter early and filter often

   Clean up Stage lines and other parts
   that should not contribute to word/phrase counting

   */
  def nonStagingContents(implicit rdd: RDD[String]): RDD[String] = rdd
    .keepOnlySpokenLines
    .removeWhiteSpaceAndMarkup

  // Words RDD: Cleansed and normalized words
  def cleansedWords(implicit rdd: RDD[String]) = nonStagingContents
    .removePunctuation
    .extractWords
    .normalizeWords

  def topWords(minLen: Int = 4)(implicit rdd: RDD[String]) = cleansedWords
    .sortByWordCountDesc(minLen)

  def topLongestWords(implicit rdd: RDD[String]) = cleansedWords
    .sortByWordLengthDescMapPartition

  /* Top 5 longest phrases: assume phrase separators are , . ; : and quoted parts contain sub-phrases
    Not an optimal solution as I am not too familiar with old english nor whether quoted phrases should be
    split up or be part of the main phrase. Did not want to over engineer this and it seems to be working
    at least for the top phrases
   */
  def topPhrases(implicit rdd: RDD[String]) = nonStagingContents
    .splitPhrasesAndSortDesc
}
