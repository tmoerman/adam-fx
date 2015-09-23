package org.tmoerman.adam.fx.snpeff

import SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype

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
    val annotatedVariants = sc.loadAnnotatedVariants(annotated).collect()

    val annotatedGenotypes = sc.loadAnnotatedGenotypes(annotated).collect()

    annotatedVariants.length shouldBe 7

    annotatedGenotypes.length shouldBe 7
  }

  "AnnotatedVariants for multi-allelic variants" should
    "only have FunctionalAnnotations for the correct alternative allele" in {

    sc.loadAnnotatedVariants(annotated)
      .collect()
      .flatMap(a => a.getAnnotations
                     .getFunctionalAnnotations
                     .map(funcAnn => (funcAnn.getAllele, a.getVariant.getAlternateAllele)))

      .forall{ case (allele, alternate) => allele == alternate }
  }

  "AnnotatedGenotypes for multi-allelic variants" should
    "only have FunctionalAnnotations for the correct alternative allele" in {

    sc.loadAnnotatedGenotypes(annotated)
      .collect()
      .flatMap(g => g.getAnnotations
                     .getFunctionalAnnotations
                     .map(funcAnn => (funcAnn.getAllele, g.getGenotype.getVariant.getAlternateAllele)))

      .forall{ case (allele, alternate) => allele == alternate }
  }

  "test for dries" should "work" in {

    case class GenotypeWithRpkm(genotype: org.tmoerman.adam.fx.avro.AnnotatedGenotype, rpkm: Double)

    val rna = sc.loadAnnotatedGenotypes(annotated)

    def pretty(genotype: Genotype): String = {
      val v = genotype.getVariant

      genotype.getSampleId + "@" +
      v.getContig.getContigName + "+" +
      v.getStart.toString + ":" +
      v.getReferenceAllele + ">" +
      v.getAlternateAllele
    }

    val rnaMap1 = rna.map(v => (pretty(v.getGenotype), GenotypeWithRpkm(v, v.getAnnotations.getFunctionalAnnotations.map(x => 0).max)))

    val rnaMap2 = rna.map(v => (pretty(v), GenotypeWithRpkm(v, v.annotations.map(annotations => annotations.functionalAnnotations.map(x => 0.0).max).getOrElse(0))))
  }

}
