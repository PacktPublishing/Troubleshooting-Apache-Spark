package com.tomekl007.chapter_3

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class JoinStrategies extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("Should join using inner join strategy") {
    //given
    val transactions = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val persons = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when
    val res = transactions.join(persons).collect().toList

    //then
    res should contain theSameElementsAs
      List((2, ("dog", "Michael")), (1, ("bag", "Tom")))
  }

  test("Should join and retain all persons when right join") {
    //given
    val transactions = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val persons = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when
    val res = transactions.rightOuterJoin(persons).collect().toList

    //then
    res should contain theSameElementsAs
      List(
        (2, (Some("dog"), "Michael")),
        (1, (Some("bag"), "Tom")),
        (3, (None, "Johnny"))
      )
  }


  test("Should join and retain all transactions when left join") {
    //given
    val transactions = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val persons = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when
    val res = transactions.leftOuterJoin(persons).collect().toList

    //then
    res should contain theSameElementsAs
      List(
        (2, ("dog", Some("Michael"))),
        (1, ("bag", Some("Tom"))),
        (4, ("car", None))
      )
  }


  test("Should join and retain all transactions and persons when outer join") {
    //given
    val transactions = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val persons = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when
    val res = transactions.fullOuterJoin(persons).collect().toList

    //then
    res should contain theSameElementsAs
      List(
        (2, (Some("dog"), Some("Michael"))),
        (1, (Some("bag"), Some("Tom"))),
        (3, (None, Some("Johnny"))),
        (4, (Some("car"), None))
      )
  }

}
