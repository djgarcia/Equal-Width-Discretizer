package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class EqualWidthDiscretizerModel(val thresholds: Array[Array[Double]]) extends Serializable {

  private def assignDiscreteValue(value: Double, thresholds: Seq[Double]): Double = {
    if (thresholds.isEmpty) {
      value
    } else {
      val ret = thresholds.indexWhere {
        value <= _
      }
      if (ret == -1) {
        thresholds.size.toDouble
      } else {
        ret.toDouble
      }
    }
  }

  def getThresholds: Array[Array[Double]] = thresholds

  def discretize(data: RDD[LabeledPoint]): RDD[LabeledPoint] = {

    val discretizedData = data.map { l =>
      val features = l.features.toArray
      val newValues = for (i <- features.indices)
        yield assignDiscreteValue(features(i), thresholds(i).toSeq)
      LabeledPoint(l.label, Vectors.dense(newValues.toArray))
    }

    discretizedData
  }
}
