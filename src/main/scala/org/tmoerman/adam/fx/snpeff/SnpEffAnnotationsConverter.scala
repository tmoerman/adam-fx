package org.tmoerman.adam.fx.snpeff

import htsjdk.variant.variantcontext.{VariantContext => BroadVariantContext}
import org.bdgenomics.adam.converters.VariantContextConverter
import org.tmoerman.adam.fx.avro.{AnnotatedGenotype, AnnotatedVariant, SnpEffAnnotations}
import org.tmoerman.adam.fx.avro.AnnotatedGenotype.{newBuilder => newAnnotatedGenotypeBuilder}
import org.tmoerman.adam.fx.avro.SnpEffAnnotations.{newBuilder => newSnpEffAnnotationsBuilder}
import org.tmoerman.adam.fx.avro.AnnotatedVariant.{newBuilder => newAnnotatedVariantBuilder}
//import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations
import SnpEffAnnotationsParser.fill
import scala.collection.JavaConversions._

/**
 * @author Thomas Moerman
 */
object SnpEffAnnotationsConverter {
  
  implicit def pimp(converter: VariantContextConverter): SnpEffAnnotationsConverter = new SnpEffAnnotationsConverter(converter)
  
}

class SnpEffAnnotationsConverter(val converter: VariantContextConverter) extends Serializable {

  def toAnnotatedGenotypes(broadVariantContext: BroadVariantContext): Seq[AnnotatedGenotype] = {
    converter
      .convert(broadVariantContext)
      .flatMap(_.genotypes)
      .map(genotype => newAnnotatedGenotypeBuilder()
                         .setGenotype(genotype)
                         .setAnnotations(buildSnpEffAnnotations(broadVariantContext, genotype.getVariant.getAlternateAllele))
                         .build())
  }

  def toAnnotatedVariants(broadVariantContext: BroadVariantContext): Seq[AnnotatedVariant] = {
    converter
      .convert(broadVariantContext)
      .map(_.variant)
      .map(variant => newAnnotatedVariantBuilder()
                        .setVariant(variant)
                        .setAnnotations(buildSnpEffAnnotations(broadVariantContext, variant.getAlternateAllele))
                        .build())
  }

  private def buildSnpEffAnnotations(broadVariantContext: BroadVariantContext, alternateAllele: String): SnpEffAnnotations = {
    val snpEffAnnotations = newSnpEffAnnotationsBuilder().build()
    fill(broadVariantContext, snpEffAnnotations)
    filterFunctionalAnnotationsByAllele(snpEffAnnotations, broadVariantContext, alternateAllele)

    snpEffAnnotations
  }

  private def filterFunctionalAnnotationsByAllele(
      snpEffAnnotations: SnpEffAnnotations,
      broadVariantContext: BroadVariantContext,
      alternateAllele: String): Unit = {

    if (broadVariantContext.getNAlleles >= 2) { // a modest optimization
      val filtered =
        snpEffAnnotations
          .getFunctionalAnnotations
          .filter(_.getAllele == alternateAllele)

      snpEffAnnotations.setFunctionalAnnotations(filtered)
    }
  }

}
