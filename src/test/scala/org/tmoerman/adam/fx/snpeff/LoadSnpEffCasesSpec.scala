package org.tmoerman.adam.fx.snpeff

import SnpEffContext._

/**
 * @author Thomas Moerman
 */
class LoadSnpEffCasesSpec extends BaseSparkContextSpec {

  val snpEffCases = "src/test/resources/cases.snpEff.vcf"

  val rdd = sc.loadAnnotatedVariants(snpEffCases).cache()

  val all = rdd.collect().toList.toArray

  "AnnotatedVariants" should "correctly have functionalAnnotations" in {
    val (ann, nop, lof, nmd, lofnmd) = all match { case Array(a, b, c, d, e) => (a, b, c, d, e) }

    ann.getAnnotations.getFunctionalAnnotations should not be empty
    ann.getAnnotations.getLossOfFunction        shouldBe empty
    ann.getAnnotations.getNonsenseMediatedDecay shouldBe empty
    ann.getAnnotations.getClinvarAnnotations    shouldBe null
    ann.getAnnotations.getDbSnpAnnotations      shouldBe null

    lof.getAnnotations.getFunctionalAnnotations shouldBe empty
    lof.getAnnotations.getLossOfFunction        should not be empty
    lof.getAnnotations.getNonsenseMediatedDecay shouldBe empty

    nmd.getAnnotations.getFunctionalAnnotations  shouldBe empty
    nmd.getAnnotations.getLossOfFunction         shouldBe empty
    nmd.getAnnotations.getNonsenseMediatedDecay  should not be empty

    lofnmd.getAnnotations.getFunctionalAnnotations shouldBe empty
    lofnmd.getAnnotations.getLossOfFunction        should not be empty
    lofnmd.getAnnotations.getNonsenseMediatedDecay should not be empty
  }

  val richRDD = rdd.toRichAnnotatedVariants

  val allRich = richRDD.collect().toList.toArray

  "RichAnnotated Variants" should "correctly have annotations as an option" in {
    val (ann, nop, lof, nmd, lofnmd) = allRich match { case Array(a, b, c, d, e) => (a, b, c, d, e) }

    ann.annotations should not be empty

    nop.annotations shouldBe empty

    lof.annotations should not be empty

    nmd.annotations should not be empty

    lofnmd.annotations should not be empty

  }

}