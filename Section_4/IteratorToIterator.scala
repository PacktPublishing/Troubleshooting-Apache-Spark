package com.tomekl007.chapter_4

import com.tomekl007.chapter_3.UserTransaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class IteratorToIterator extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use iterator-to-iterator") {
    //given
    val data = spark
      .parallelize(List(
        UserTransaction("a", 100),
        UserTransaction("b", 101),
        UserTransaction("a", 202),
        UserTransaction("b", 1),
        UserTransaction("c", 55)
      )
      ).keyBy(_.userId)
      .partitionBy(new Partitioner {
        override def numPartitions: Int = 3

        override def getPartition(key: Any): Int = {
          key.hashCode % 3
        }
      })

    println(data.partitions.length)

    //when
    val res = data.mapPartitions[Long](iter =>
      iter.map(_._2).map(_.amount)
    ).collect().toList

    //then
    res should contain theSameElementsAs List(55, 100, 202, 101, 1)
  }
}
