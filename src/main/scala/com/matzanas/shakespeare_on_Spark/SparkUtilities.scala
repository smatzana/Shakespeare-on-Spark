package com.matzanas.shakespeare_on_Spark

import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.SortedMap


object SparkUtilities {

  /*
    Some files have weird character sequences in them that trip the Hadoop/Spark
    file reader. Replacing those with just space characters
    and convert to String
   */
   def readAndScrubUTF16(line: (LongWritable, Text)) : String = {
      var b: Array[Byte] = line._2.copyBytes()
      while(b.containsSlice(List(0x00, 0x14, 0x20))){
        val i = b.lastIndexOfSlice(List(0x00, 0x14, 0x20))
        b = b.take(i+1) ++  Seq[Byte](0x20, 0x00) ++ b.drop(i+3)
      }
      b = if (b.last == 0) b.toList.dropRight(1).toArray else b
      (new String(b, 0, b.length, "UTF-16")).trim
  }

  def removeLinePunctuation(s: String) = s.replaceAll("\\p{Punct}", " ")


  def cleanseInputLine(s: String) =
    s.replaceAll("[\n\r]", "").replaceAll("[\\s]+", " ").replaceAll(".*>\\s+", "")
  
  def pullTopFiveFromIterator(i: Iterator[String]) = {

    implicit val Ord = implicitly[Ordering[Int]]

    val tuples = for (
      s <- i;
      trimmed = s.trim
    ) yield (trimmed.length -> trimmed)
    val sortedMap = SortedMap.apply(tuples.toList: _*)(Ord.reverse)
    sortedMap.take(5).toIterator
  }

  /*
    Replace any quotes in non quoted words, like 't, 'twixt etc with /
    Assume quoted phrases start with an Upper Case letter.
    Then replace all quotes with dot (.) so they can be picked up as phrases
   */
  def normalizeQuotes(w: String) =
    w.replaceAll("(\\p{Alpha})'([sd])", "$1/$2")
      .replaceAll("s'", "s/")
      .replaceAll("'(\\p{Lower}+)", "/$1")
      .replaceAll("'", ".")
}
