package org.tmoerman.adam.fx.snpeff

import SnpEffContext._

/**
 * @author Thomas Moerman
 */
class LoadCornerCasesSpec extends BaseSparkContextSpec {

  val casesVcf = "src/test/resources/cases.snpEff.vcf"

  lazy val variantCtxs = sc.loadVariantsWithSnpEffAnnotations(casesVcf)

  lazy val all = variantCtxs.collect().toList.toArray

  "variantContexts" should "correctly have optional snpEffAnnotations" in {
    all match {
      case Array(ann, nop, lof, nmd, lofnmd) =>

        ann.snpEffAnnotations    shouldBe defined
        nop.snpEffAnnotations    shouldBe empty
        lof.snpEffAnnotations    shouldBe defined
        nmd.snpEffAnnotations    shouldBe defined
        lofnmd.snpEffAnnotations shouldBe defined
    }
  }
  
  "variantContexts" should "correctly have functionalAnnotations" in {
    all match {
      case Array(ann, _, lof, nmd, lofnmd) =>

        ann.snpEffAnnotations.get.functionalAnnotations  should not be empty
        ann.snpEffAnnotations.get.lossOfFunction         shouldBe empty
        ann.snpEffAnnotations.get.nonsenseMediatedDecay  shouldBe empty

        lof.snpEffAnnotations.get.functionalAnnotations  shouldBe empty
        lof.snpEffAnnotations.get.lossOfFunction         shouldBe defined
        lof.snpEffAnnotations.get.nonsenseMediatedDecay  shouldBe empty

        nmd.snpEffAnnotations.get.functionalAnnotations  shouldBe empty
        nmd.snpEffAnnotations.get.lossOfFunction         shouldBe empty
        nmd.snpEffAnnotations.get.nonsenseMediatedDecay  shouldBe defined

        lofnmd.snpEffAnnotations.get.functionalAnnotations  shouldBe empty
        lofnmd.snpEffAnnotations.get.lossOfFunction         shouldBe defined
        lofnmd.snpEffAnnotations.get.nonsenseMediatedDecay  shouldBe defined
    }
  }

}