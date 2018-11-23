package com.tomekl007.chapter_2

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class PartitionsTest extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should load data partitioned") {
    //given
    val input = spark.makeRDD(0 to 100000)

    //when
    assert(input.partitions.length == 2)
    println(s"number of partitions: ${input.partitions.length}")
    input.filter(_ > 100).map(_ * 100).collect().toList
    
    //then question: on how many executors that computation can be distributed
    //and calculated in parallel?
  }


  test("change number of partitions to make computation parallel to more executors") {
    //given
    val input = spark.makeRDD(0 to 100000).coalesce(10)

    //then
    assert(input.partitions.length == 2)
    println(s"number of partitions: ${input.partitions.length}")
    input.filter(_ > 100).map(_ * 100).collect().toList

    //beware! coalesce does not change number of partitions upwards, only downwards
  }

  test("repartition() to make computation parallel to more executors") {
    //given
    val input = spark.makeRDD(0 to 100000).repartition(10)

    //then
    assert(input.partitions.length == 10)
    println(s"number of partitions: ${input.partitions.length}")
    input.filter(_ > 100).map(_ * 100).collect().toList

    //repartition to make your computations more parallel
  }



}