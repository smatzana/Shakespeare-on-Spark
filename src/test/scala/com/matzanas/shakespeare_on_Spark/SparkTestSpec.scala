package com.matzanas.shakespeare_on_Spark

import com.matzanas.shakespeare_on_Spark.ShakespeareOnSpark.shakespeareSparkAccumulator
import com.matzanas.shakespeare_on_Spark.RDDOps._
import com.matzanas.shakespeare_on_Spark.SparkJobs._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class SparkTestSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
    shakespeareSparkAccumulator = new ShakespeareSparkAccumulator
    sc.register(shakespeareSparkAccumulator, "shakespeareSparkAccumulator")
  }

  "Lines starting with >" should "be discarded" in {
    val lines = Array("<ACT 1>", "Now, fair Hippolyta, our nuptial hour", "</ACT1>")
    val spokenLines = sc
      .parallelize(lines)
      .keepOnlySpokenLines
      .collect

    spokenLines.length should be(1)
    spokenLines(0) should equal(lines(1))
  }

  "White space and markup" should "be removed" in {
    val lines = Array("%1>   Four days will    quickly steep themselves in night;\r\n   Four nights will quickly dream away the time;")
    val cleanLines = sc
      .parallelize(lines)
      .removeWhiteSpaceAndMarkup
      .collect

    cleanLines.length should be(1)
    cleanLines(0) should equal("Four days will quickly steep themselves in night; Four nights will quickly dream away the time;")
  }

  "Punctuation" should "be removed" in {
    val lines = Array("Four days will, quickly: steep. themselves in night; Four nights will quickly dream away the time;")
    val noPunctuation = sc
      .parallelize(lines)
      .removePunctuation
      .collect

    noPunctuation.length should be(1)
    noPunctuation(0) should equal("Four days will  quickly  steep  themselves in night  Four nights will quickly dream away the time ")
  }

  "Find top five words" should "work" in {
    val lines = Array("The Four days Will, the quickly: steep. The themselves in night; Four nights will quickly dream away the time; Will do")
    val rdd = sc.parallelize(lines)
    val topFiveWords = topWords()(rdd).take(5)


    topFiveWords.length should be(5)
    topFiveWords(0)._1 should be("will")
    topFiveWords(0)._2 should be(3)
  }

  "Find longest words" should "work" in {
     val lines = Array("Full of vexation come I, with complaint",
      "Against my child, my daughter Hermia.",
      "Stand forth, Demetrius. My noble lord,",
      "This man hath my consent to marry her.",
      "Stand forth, Lysander: and, my gracious duke,",
      "This man hath bewitch'd the bosom of my child:",
      "Thou, thou, Lysander, thou hast given her rimes,",
      "And love-tokens with my child;",
      "Thou hast by moonlight at her window sung,",
      "With feigning voice, verses of feigning love;",
      "And stol'n the impression of her fantasy",
      "With bracelets of thy hair, rings, gawds, conceits,",
      "Knacks, trifles, nosegays, sweetmeats, messengers",
      "Of strong prevailment in unharden'd youth;",
      "With cunning hast thou filch'd my daughter's heart;",
      "Turn'd her obedience, which is due to me,",
      "To stubborn harshness. And, my gracious duke,",
      "Be it so she will not here before your Grace",
      "Consent to marry with Demetrius,",
      "I beg the ancient privilege of Athens,",
      "As she is mine, I may dispose of her;",
      "Which shall be either to this gentleman,",
      "Or to her death, according to our law",
      "Immediately provided in that case.")

    val rdd = sc.parallelize(lines)

    val topLength = topLongestWords(rdd).take(5)

    topLength.length should be(5)
    topLength(0) should be((11, "prevailment"))
    topLength(1) should be((11, "immediately"))
    topLength(2) should be((10, "impression"))
    topLength(3) should be((10, "messengers"))
    topLength(4) should be((9, "demetrius"))
  }

  "Find longest phrases" should "work" in {

    val rdd = sc.parallelize(
      Array("<EGEUS> <2%>",
      "        Full of vexation come I, with complaint Against my child, my daughter Hermia.  Stand forth, Demetrius. My noble lord, This man hath my consent to marry her.  Stand forth, Lysander: and, my gracious duke, This man hath bewitch'd the bosom of my child: Thou, thou, Lysander, thou hast given her rimes, And interchang'd love-tokens with my child; Thou hast by moonlight at her window sung, With feigning voice, verses of feigning love; And stol'n the impression of her fantasy With bracelets of thy hair, rings, gawds, conceits, Knacks, trifles, nosegays, sweetmeats, messengers Of strong prevailment in unharden'd youth; With cunning hast thou filch'd my daughter's heart; Turn'd her obedience, which is due to me, To stubborn harshness. And, my gracious duke, Be it so she will not here before your Grace Consent to marry with Demetrius, I beg the ancient privilege of Athens, As she is mine, I may dispose of her; Which shall be either to this gentleman, Or to her death, according to our law Immediately provided in that case.",
      "</EGEUS>",
      "",
      "<THESEUS>       <3%>",
      "        What say you, Hermia? be advis'd, fair maid.  To you, your father should be as a god; One that compos'd your beauties, yea, and one To whom you are but as a form in wax By him imprinted, and within his power To leave the figure or disfigure it.  Demetrius is a worthy gentleman.",
      "</THESEUS>",
      "",
      "<HERMIA>        <3%>",
      "        I would my father look'd but with my eyes.",
      "</HERMIA>",
      "",
      "<THESEUS>       <3%>",
      "        Rather your eyes must with his judgment look.",
      "</THESEUS>",
      "<THESEUS>       <4%>",
      "        Take time to pause; and, by the next new moon,— The sealing-day betwixt my love and me For everlasting bond of fellowship,— Upon that day either prepare to die For disobedience to your father's will, Or else to wed Demetrius, as he would; Or on Diana's altar to protest For aye austerity and single life.",
      "</THESEUS>",
      "",
      "<DEMETRIUS>     <5%>",
      "        Relent, sweet Hermia; and, Lysander, yield Thy crazed title to my certain right.",
      "</DEMETRIUS>",
      "",
      "<LYSANDER>      <5%>",
      "        You have her father's love, Demetrius; Let me have Hermia's: do you marry him.",
      "</LYSANDER>"))

    val longestPhrases = topPhrases(rdd).take(5)

    longestPhrases.length should be(5)
    longestPhrases(0).s should be("and, by the next new moon,— The sealing-day betwixt my love and me For everlasting bond of fellowship,— Upon that day either prepare to die For disobedience to your father/s will, Or else to wed Demetrius, as he would")
    longestPhrases(0).count should be(218)
  }
}
