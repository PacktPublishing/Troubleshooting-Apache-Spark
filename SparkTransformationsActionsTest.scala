package com.tomekl007

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class SparkTransformationsActionsTest extends FunSuite {

  private val spark = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should map and filter values") {
    //given
    val inputRDD = spark.makeRDD(List(1, 2, 3, 4))

    //when
    val res = inputRDD
      .map(_ * 2) //transformation
      .filter(_ > 6) //transformation
      .collect() //this is action - triggers processing
      .toList

    //then
    res should contain theSameElementsAs List(
      8
    )
  }

  test("should group by key and map only values") {
    //given
    val inputRDD = spark.makeRDD(List(
      DomainObject(1, "a"),
      DomainObject(2, "b"),
      DomainObject(1, "c")
    ))

    //when
    val res = inputRDD
      .keyBy(_.id) //transformation
      .groupByKey() //transformation
      .sortBy(_._2.size, ascending = false) //transformation
      .take(1) //action
      .map { case (id, iter) => (id, iter.toList) }

    //then
    res should contain theSameElementsAs Array(
      (1, List(
        DomainObject(1, "a"),
        DomainObject(1, "c")
      ))
    )
  }

  test("should reduce calculating sum") {
    //given
    val inputRDD = spark.makeRDD(List(
      1, 2, 3, 4
    ))

    //when
    val res = inputRDD
      .reduce { case (cur, sum) => cur + sum } //action


    //then
    assert(res == 10)
  }


  test("should map values only") {
    //given
    val inputRDD = spark.makeRDD(List(
      DomainObject(1, "first desc"),
      DomainObject(2, "second desc")
    ))

    //when
    val res = inputRDD
      .map(d => (d.id, d.desc)) //transformation
      .mapValues(_.toUpperCase) //transformation
      .collect() //action
      .toList

    //then
    res should contain theSameElementsAs List(
      (1, "FIRST DESC"),
      (2, "SECOND DESC")
    )
  }
}

case class DomainObject(id: Int, desc: String)
