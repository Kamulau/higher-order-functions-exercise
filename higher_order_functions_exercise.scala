import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import spark.implicits._

val arr1 = Array(1, 2, 3, 4, 5)
val arr2 = Array(2, 3, 4, 5, 6)
val df = Seq(("a",arr1),("b",arr2)).toDF("key", "numbers")

val explodeAggDf = df
  .withColumn("number", explode('numbers))
  .groupBy("key")
  .agg(mean('number).alias("mean"))

case class KeyMeanPair(key: String, mean: Double)

val mapDf = df.map {
    case Row(key: String, numbers: Seq[Integer]) => 
        val mean = numbers.foldLeft(0)(_ + _).toDouble/numbers.size
        KeyMeanPair(key, mean)
}

val arrayAvgUdf = udf(
    (numbers: Seq[Integer]) => {
      numbers.foldLeft(0)(_ + _).toDouble/numbers.size
    })

val udfDf = df.select('key, arrayAvgUdf('numbers).alias("mean"))

val higherOrderFuncDf = df.select(
    'key, (expr("aggregate(numbers, CAST(0 AS DOUBLE), (x, y) -> x+y)")/
        size('numbers)).alias("mean"))

//Result check
explodeAggDf.show
mapDf.show
udfDf.show
higherOrderFuncDf.show

//Physical Plan check
explodeAggDf.explain
mapDf.explain
udfDf.explain
higherOrderFuncDf.explain

//Below is Spark 3.0.0 (preview)
val hofScalaDf = df.select('key, aggregate('numbers, lit(0.0), (x, y) => x+y, f => f/size('numbers)))
