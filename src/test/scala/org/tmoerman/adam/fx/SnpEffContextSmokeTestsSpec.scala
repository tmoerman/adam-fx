package org.tmoerman.adam.fx

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.adam.fx.snpEff.EffectsContext

class SnpEffContextSmokeTestsSpec extends FlatSpec with Matchers {

  val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
    .set("spark.kryo.registrator", "org.tmoerman.adam.fx.serialization.AdamFxKryoRegistrator")
    .set("spark.kryo.referenceTracking", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  val ec = new EffectsContext(sc)

  val simpleVcf = "src/test/resources/small.vcf"

  val annotatedVcf = "src/test/resources/small.snpEff.vcf"

  "Loading SnpEffAnnotations" should "pass the smoke test on an annotated file" in {
    val annotations = ec.loadSnpEffAnnotations(annotatedVcf)
    val all = annotations.collect()

    all.exists(_.getFunctionalAnnotations.isEmpty) shouldBe false
  }

  "Loading SnpEffAnnotations" should "pass the smoke test on a non-annotated file" in {
    val annotations = ec.loadSnpEffAnnotations(simpleVcf)
    val all = annotations.collect()

    all.forall(_.getFunctionalAnnotations.isEmpty) shouldBe true
  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an annotated file" in {
    val variantCtxs = ec.loadVariantsWithSnpEffAnnotations(annotatedVcf)
    val all = variantCtxs.collect()

    all.forall(_.snpEffAnnotations.isDefined) shouldBe true
  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an non-annotated file" in {
    val variantCtxs = ec.loadVariantsWithSnpEffAnnotations(simpleVcf)
    val all = variantCtxs.collect()

    all.forall(_.snpEffAnnotations.isEmpty) shouldBe true
  }

}