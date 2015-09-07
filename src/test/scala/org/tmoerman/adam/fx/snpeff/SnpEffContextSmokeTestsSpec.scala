package org.tmoerman.adam.fx.snpeff

import SnpEffContext._

class SnpEffContextSmokeTestsSpec extends BaseSparkContextSpec {

  val smallVcf = "src/test/resources/small.vcf"

  val smallAnnotatedVcf = "src/test/resources/small.snpEff.vcf"

// TODO revise disarmed tests

//  val smallAnnotatedParquet = "src/test/resources/small.snpEff.adam"

  "Loading SnpEffAnnotations" should "pass the smoke test on a non-annotated file" in {
    val annotations = sc.loadSnpEffAnnotations(smallVcf)
    val all = annotations.collect()

    all.forall(_.getFunctionalAnnotations.isEmpty) shouldBe true
  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an non-annotated file" in {
    val variantCtxs = sc.loadVariantsWithSnpEffAnnotations(smallVcf)
    val all = variantCtxs.collect()

    all.forall(_.snpEffAnnotations.isEmpty) shouldBe true
  }

  "Loading SnpEffAnnotations" should "pass the smoke test on an annotated .vcf file" in {
    val annotations = sc.loadSnpEffAnnotations(smallAnnotatedVcf)
    val all = annotations.collect()

    all.exists(_.getFunctionalAnnotations.isEmpty) shouldBe false
  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an annotated .vcf file" in {
    val variantCtxs = sc.loadVariantsWithSnpEffAnnotations(smallAnnotatedVcf)
    val all = variantCtxs.collect()

    all.forall(_.snpEffAnnotations.isDefined) shouldBe true
  }

//  "Loading SnpEffAnnotations" should "pass the smoke test on an annotated .adam file" in {
//    val annotations = sc.loadSnpEffAnnotations(smallAnnotatedParquet)
//    val all = annotations.collect()
//
//    all.exists(_.getFunctionalAnnotations.isEmpty) shouldBe false
//  }
//
//  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an annotated .adam file" in {
//    val variantCtxs = sc.loadVariantsWithSnpEffAnnotations(smallAnnotatedParquet)
//    val all = variantCtxs.collect()
//
//    all.forall(_.snpEffAnnotations.isDefined) shouldBe true
//  }
  
}