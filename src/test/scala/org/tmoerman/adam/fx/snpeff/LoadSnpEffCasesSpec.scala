package org.tmoerman.adam.fx.snpeff

import SnpEffContext._

/**
 * @author Thomas Moerman
 */
class LoadSnpEffCasesSpec extends BaseSparkContextSpec {

  val snpEffCases = "src/test/resources/cases.snpEff.vcf"

  val all = sc.loadVariantsWithSnpEffAnnotations(snpEffCases).collect().toList.toArray

  val (ann, nop, lof, nmd, lofnmd) = all match { case Array(a, b, c, d, e) => (a, b, c, d, e) }

  "variantContexts" should "correctly have optional snpEffAnnotations" in {
    ann.snpEffAnnotations    shouldBe defined
    nop.snpEffAnnotations    shouldBe empty
    lof.snpEffAnnotations    shouldBe defined
    nmd.snpEffAnnotations    shouldBe defined
    lofnmd.snpEffAnnotations shouldBe defined
  }
  
  "variantContexts" should "correctly have functionalAnnotations" in {
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