package org.tmoerman.adam.fx.snpeff

import SnpEffContext._

class SnpEffContextSmokeTestsSpec extends BaseSparkContextSpec {

  val smallVcf = "src/test/resources/small.vcf"

  val smallAnnotatedVcf = "src/test/resources/small.snpEff.vcf"

  "Loading AnnotatedVariants" should "pass the smoke test on a non-annotated file" in {
    val annotatedVariants = sc.loadAnnotatedVariants(smallVcf)
    val all = annotatedVariants.collect()

    all.forall(_.getAnnotations.getFunctionalAnnotations.isEmpty) shouldBe true
  }

  "Loading AnnotatedGenotypes" should "pass the smoke test on a non-annotated file" in {
    val annotatedGenotypes = sc.loadAnnotatedGenotypes(smallVcf)
    val all = annotatedGenotypes.collect()

    all.forall(_.getAnnotations.getFunctionalAnnotations.isEmpty) shouldBe true
  }

  "Loading AnnotatedVariants" should "pass the smoke test on an annotated .vcf file" in {
    val annotatedVariants = sc.loadAnnotatedVariants(smallAnnotatedVcf)
    val all = annotatedVariants.collect()

    all.exists(_.getAnnotations.getFunctionalAnnotations.isEmpty) shouldBe false
  }

  "Loading AnnotatedGenotypes" should "pass the smoke test on an annotated .vcf file" in {
    val annotatedGenotypes = sc.loadAnnotatedGenotypes(smallAnnotatedVcf)
    val all = annotatedGenotypes.collect()

    all.exists(_.getAnnotations.getFunctionalAnnotations.isEmpty) shouldBe false
  }

}