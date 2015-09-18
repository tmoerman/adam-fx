package org.tmoerman.adam.fx.snpeff

import SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.tmoerman.adam.fx.snpeff.model.{RichSnpEffAnnotations, VariantContextWithSnpEffAnnotations}

/**
 * @author Thomas Moerman
 */
class MultiAllelicSiteSpec extends BaseSparkContextSpec {

  val raw = "src/test/resources/multi_allelic.vcf"

  "Loading genotypes of raw multi-allelic variants" should "yield the correct number of instances" in {
    val variantContexts = sc.loadGenotypes(raw, None).toVariantContext().collect()

    variantContexts.length shouldBe 7
  }

  val annotated = "src/test/resources/multi_allelic.annotated.vcf"

  "Loading SnpEffAnnotations for multi-allelic variants" should "yield the correct number of instances" in {
    val snpEffAnnotations = sc.loadSnpEffAnnotations(annotated).collect()

    val richVariants = sc.loadVariantsWithSnpEffAnnotations(annotated).collect()

    val variants = sc.loadVariants(annotated).collect()

    snpEffAnnotations.length shouldBe 7

    richVariants.length shouldBe 7

    variants.length shouldBe 7
  }

  "SnpEffAnnotations for multi-allelic variants" should
    "only have FunctionalAnnotations for the correct alternative allele" in {

    sc.loadSnpEffAnnotations(annotated)
      .collect()
      .forall(a => a.getFunctionalAnnotations
      .forall(_.getAllele == a.getVariant.getAlternateAllele)) shouldBe true
  }

  "VariantContextsWithSnpEffAnnotations for multi-allelic variants" should
    "only have FunctionalAnnotations for the correct alternative allele" in {

    sc.loadVariantsWithSnpEffAnnotations(annotated)
      .collect()
      .forall(v => v.snpEffAnnotations.map(a => a.functionalAnnotations
                                                 .forall(_.getAllele == a.variant.getAlternateAllele)).get) shouldBe true
  }

}
