package com.matzanas.shakespeare_on_Spark

import org.apache.spark.util.AccumulatorV2

final case class ShakespeareMetrics(var lines: Long, var words: Long) {
  def add(shakespeareCounter: ShakespeareMetrics) = {
    lines += shakespeareCounter.lines
    words += shakespeareCounter.words
  }

  override def toString = s"Total lines read: ${lines}, totalWords parsed: ${words}"
}

class ShakespeareSparkAccumulator extends AccumulatorV2[ShakespeareMetrics, ShakespeareMetrics ]{

  private val shakespeareCounter = ShakespeareMetrics(0, 0)

  override def isZero: Boolean = shakespeareCounter.lines == 0 && shakespeareCounter.words == 0

  override def copy(): AccumulatorV2[ShakespeareMetrics, ShakespeareMetrics] = new ShakespeareSparkAccumulator()

  override def reset(): Unit = {
    shakespeareCounter.words = 0
    shakespeareCounter.lines = 0
  }

  override def add(v: ShakespeareMetrics): Unit = shakespeareCounter.add(v)

  override def merge(other: AccumulatorV2[ShakespeareMetrics, ShakespeareMetrics]): Unit = shakespeareCounter.add(other.asInstanceOf[ShakespeareMetrics])

  override def value: ShakespeareMetrics = shakespeareCounter
}
