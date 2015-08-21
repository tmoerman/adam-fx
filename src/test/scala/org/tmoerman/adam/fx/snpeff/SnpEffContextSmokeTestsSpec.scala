package org.tmoerman.adam.fx.snpeff

class SnpEffContextSmokeTestsSpec extends SnpEffContextSpec {

  val smallVcf = "src/test/resources/small.vcf"

  val smallAnnotatedVcf = "src/test/resources/small.snpEff.vcf"

  "Loading SnpEffAnnotations" should "pass the smoke test on an annotated file" in {
    val annotations = ec.loadSnpEffAnnotations(smallAnnotatedVcf)
    val all = annotations.collect()

    all.exists(_.getFunctionalAnnotations.isEmpty) shouldBe false
  }

  "Loading SnpEffAnnotations" should "pass the smoke test on a non-annotated file" in {
    val annotations = ec.loadSnpEffAnnotations(smallVcf)
    val all = annotations.collect()

    all.forall(_.getFunctionalAnnotations.isEmpty) shouldBe true
  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an annotated file" in {
    val variantCtxs = ec.loadVariantsWithSnpEffAnnotations(smallAnnotatedVcf)
    val all = variantCtxs.collect()

    all.forall(_.snpEffAnnotations.isDefined) shouldBe true
  }

  "Loading VariantContextWithSnpEffAnnotations" should "pass the smoke test on an non-annotated file" in {
    val variantCtxs = ec.loadVariantsWithSnpEffAnnotations(smallVcf)
    val all = variantCtxs.collect()

    all.forall(_.snpEffAnnotations.isEmpty) shouldBe true
  }
  
}