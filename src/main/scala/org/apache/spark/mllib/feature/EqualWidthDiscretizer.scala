package org.apache.spark.mllib.feature

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD

class EqualWidthDiscretizer(val data: RDD[LabeledPoint], val nBins: Int) extends Serializable {

  def calcThresholds(): EqualWidthDiscretizerModel = {

    val summaryTrain: MultivariateStatisticalSummary = Statistics.colStats(data.map(_.features))
    val size = data.first.features.size
    val range: Array[Double] = new Array(size)

    for (i <- 0 to size - 1) {
      range(i) = summaryTrain.max(i) - summaryTrain.min(i)
    }

    val splits = range.map(_ / nBins.toDouble)
    val thresholds: Array[Array[Double]] = new Array[Array[Double]](size)

    for (c <- 0 to size - 1) {
      thresholds(c) = new Array(nBins + 1)
      thresholds(c) = thresholds(c).zipWithIndex.map { case (v, k) =>
        k * splits(c) + summaryTrain.min(c)
      }
    }

    new EqualWidthDiscretizerModel(thresholds)
  }
}
