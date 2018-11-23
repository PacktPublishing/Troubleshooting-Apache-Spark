package com.tomekl007.chapter_2

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class LogicalVsPhysicalPlan extends FunSuite {
  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  test("should explain plan showing logical and physical with RDD") {
    def incrementFunction = (i: Double) => i + 1

    def multiplyByTwo = (i: Double) => i * 2

    val d = spark
      .sparkContext.makeRDD(List(1.0, 2.0))

    val plan = d
      .map(incrementFunction andThen multiplyByTwo)
      .toDebugString

    println(plan)
  }

  test("should explain plan showing logical and physical with UDF and DF") {
    import org.apache.spark.sql.functions.udf
    def incrementFunction = udf((i: Double) => i + 1)

    def multiplyByTwo = udf((i: Double) => i * 2)

    val d = spark
      .createDataFrame(Seq((1, "some value not important"), (2, "some value not important2")))
      .toDF("rating", "description")
    val q = d.withColumn("inc_mult", multiplyByTwo(incrementFunction(d("rating"))))

    q.explain(true)
  }


}
