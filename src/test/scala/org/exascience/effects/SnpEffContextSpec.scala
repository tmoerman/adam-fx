package org.exascience.effects

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FlatSpec}

class SnpEffContextSpec extends FlatSpec with Matchers {

  val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[4]")
    .set("spark.kryo.registrator", "org.exascience.effects.serialization.AdamEffectsKryoRegistrator")
    .set("spark.kryo.referenceTracking", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  val ec = new EffectsContext(sc)

  val testFile = "src/test/resources/snpEff.small.vcf"

  "Loading SnpEffAnnotations" should "pass this smoke test" in {

    val annotations = ec.loadSnpEffAnnotations(testFile)

    val all = annotations.collect()

    println(all(0))

    println(all.last)

  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass this smoke test" in {

    val variantCtxs = ec.loadVariantsWithSnpEffAnnotations(testFile)

    val all = variantCtxs.collect()

    println(all(0))

    println(all.last)

  }

}