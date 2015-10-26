package org.tmoerman.adam.fx.spike

import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.BaseSparkContextSpec

import scala.util.Try

/**
 * @author Kevain
 */
class SpikeSpec extends BaseSparkContextSpec {

  type Paar = (String, Int)

  "type aliasing" should "bla" in {

    val rdd: RDD[Paar] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))

    rdd.map{case (a, b) => }

  }

  "list tabulate" should "flatmap elegantly" in {

    val r = List("a", "b").zip(List("c", "d"))

    println(r)

  }

  "underscore" should "string interpolate" in {

    val hello = "hello"
    val infix = "infix"

    println(s"$hello$infix$hello")

  }

  "join behaviour on RDDs" should "bla" in {
    val rdd1 = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2 = sc.parallelize(List((2, "b"), (4, "d")))

    println(rdd1.join(rdd2).map(_._2).collect().toList)
  }


  "maxBy" should "behave" in {

    println(List[String]().maxBy(_.length))

  }


  "shapeless" should "allow for merging tuples" in {
    def blaa(a: Int, b: Int) = List(a)

    sc.parallelize(List((1,1), (2,2))).map(Function.tupled(blaa))
  }


  "scala-fu" should "b" in {

    val bla = "a5"

  }

}
