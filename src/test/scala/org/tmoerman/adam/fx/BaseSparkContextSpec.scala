package org.tmoerman.adam.fx

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
 * @author Thomas Moerman
 */
object BaseSparkContextSpec {

  lazy val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
    .set("spark.kryo.registrator", "org.tmoerman.adam.fx.serialization.AdamFxKryoRegistrator")
    .set("spark.kryo.referenceTracking", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  lazy val sc = new SparkContext(conf)

}

trait BaseSparkContextSpec extends FlatSpec with Matchers {

  lazy val sc = BaseSparkContextSpec.sc

}
