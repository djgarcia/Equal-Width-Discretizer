# Equal-Width-Discretizer

Equal Width Discretizer for Apache Spark.

## Example


```scala
import org.apache.spark.mllib.feature._

val nBins = 25 // Number of bins

// Data must be cached in order to improve the performance

val discretizerModel = new EqualWidthDiscretizer(data, // RDD[LabeledPoint]
                                                nBins).calcThresholds()
                                
val discretizedData = discretizerModel.discretize(data)

```
