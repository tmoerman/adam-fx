package org.tmoerman.adam.fx.snpeff

import htsjdk.variant.variantcontext.{VariantContext => BroadVariantContext}
import org.bdgenomics.adam.converters.VariantContextConverter
import org.tmoerman.adam.fx.avro.SnpEffAnnotations
import org.tmoerman.adam.fx.avro.SnpEffAnnotations._
import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations
import scala.collection.JavaConversions._

/**
 * @author Thomas Moerman
 */
object SnpEffAnnotationsConverter {
  
  implicit def pimp(converter: VariantContextConverter): SnpEffAnnotationsConverter = new SnpEffAnnotationsConverter(converter)
  
}

class SnpEffAnnotationsConverter(val converter: VariantContextConverter) extends Serializable {

  def toVariantContextsWithSnpEffAnnotations(broadVariantContext: BroadVariantContext): Seq[VariantContextWithSnpEffAnnotations] =
    toSnpEffAnnotations(broadVariantContext).map(VariantContextWithSnpEffAnnotations(_))

  def toSnpEffAnnotations(broadVariantContext: BroadVariantContext): Seq[SnpEffAnnotations] = {
    converter
      .convert(broadVariantContext)
      .map(variantContext => {val snpEffAnnotations = newBuilder().setVariant(variantContext.variant).build()
                              SnpEffAnnotationsParser.convert(broadVariantContext, snpEffAnnotations)})
      .map(snpEffAnnotations => filterFunctionalAnnotationsByAllele(broadVariantContext, snpEffAnnotations))
  }

  def filterFunctionalAnnotationsByAllele(broadVariantContext: BroadVariantContext, snpEffAnnotations: SnpEffAnnotations): SnpEffAnnotations = {
    if (broadVariantContext.getNAlleles >= 2) {
      val filtered =
        snpEffAnnotations
          .getFunctionalAnnotations
          .filter(_.getAllele == snpEffAnnotations.getVariant.getAlternateAllele)

      snpEffAnnotations.setFunctionalAnnotations(filtered)
    }

    snpEffAnnotations
  }

}
