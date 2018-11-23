package com.tomekl007.chapter_1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class RDDCaveats extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("action then filter") {
    //given
    val data = spark.makeRDD(0 to 100000)

    //when
    val now = System.currentTimeMillis()
    val res = data.map(_ - 100).takeOrdered(10)
    println(s"action then filter Taken: ${System.currentTimeMillis() - now}")

    //then
    assert(res.length == 10)
  }

  test("filter then action") {
    //given
    val data = spark.makeRDD(0 to 100000)

    //when
    val now = System.currentTimeMillis()
    val res = data.takeOrdered(10).map(_ - 100)
    println(s"filter then action Taken: ${System.currentTimeMillis() - now}")

    //then
    assert(res.length == 10)
  }

}