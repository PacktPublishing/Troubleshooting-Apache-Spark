package com.tomekl007.chapter_1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class EagerVsLazy extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("when not using action results are not calculated") {
    //given
    val input = spark.makeRDD(List(1, 2, 3, 4))

    //when apply transformation
    val rdd = input
      .filter(_ > 1)
      .map(_ * 10)


    //then what? - we are not need the result set, so it is not calculated

  }

  test("when using action results are calculated LAZY") {
    //given
    val input = spark.makeRDD(List(1, 2, 3, 4))

    //when apply transformation
    val data = input
      .filter(_ > 1)
      .map(_ * 10)

    //then - to trigger computation we need to issue action

    //expect what? - we are not need the result set, so it is not calculated
    data.collect().toList should contain theSameElementsAs
      List(20, 30, 40)

  }

}

