package com.tomekl007.chapter_4

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SetupOverheadAccumulators extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use accumulator for counter") {
    //given
    val input = spark.makeRDD(List("this", "is", "a", "sentence"))
    val operationsCounter = spark.doubleAccumulator("operations_counter")

    //when
    val transformed = input
      .map { x => operationsCounter.add(1); x.toUpperCase }
      //warning: if spark executor fails during transformation, it will re-try:
      //because of that counter will be incremented x2 for partitions that
      //were on failed executor
      .collect()

    //then
    assert(operationsCounter.count == 4)
  }
}

