package org.exascience.effects

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FlatSpec}

class VcfSpec extends FlatSpec with Matchers {

  "" should "" in {

    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[4]")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val ec = new EffectsContext(sc)

    val annotations = ec.loadSnpEffAnnotations("src/test/resources/snpEff.small.vcf")

    //print(annotations.first())

    print(annotations.collect().last)

  }

}