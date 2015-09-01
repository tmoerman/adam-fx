package org.tmoerman.adam.fx.snpeff.model

import org.bdgenomics.adam.models.{ReferencePosition, VariantContext}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.{DatabaseVariantAnnotation, Genotype}
import org.tmoerman.adam.fx.avro.SnpEffAnnotations
import org.tmoerman.adam.fx.util.ReflectToString

/**
 * Extends the Adam VariantContext type with optional SnpEffAnnotations.
 *
 * @author Thomas Moerman
 */
object VariantContextWithSnpEffAnnotations {

  def apply(snpEffAnnotations: SnpEffAnnotations): VariantContextWithSnpEffAnnotations = {
    VariantContextWithSnpEffAnnotations(VariantContext(snpEffAnnotations.getVariant), snpEffAnnotations)
  }

  def apply(variantContext: VariantContext, snpEffAnnotations: SnpEffAnnotations) = {
    new VariantContextWithSnpEffAnnotations(
      variantContext.position,
      variantContext.variant,
      variantContext.genotypes,
      variantContext.databases,
      new RichSnpEffAnnotations(snpEffAnnotations).asOption())
  }

}

case class VariantContextWithSnpEffAnnotations(override val position: ReferencePosition,
                                               override val variant: RichVariant,
                                               override val genotypes: Iterable[Genotype],
                                               override val databases: Option[DatabaseVariantAnnotation] = None,
                                               snpEffAnnotations: Option[RichSnpEffAnnotations] = None)
  extends VariantContext(position, variant, genotypes, databases) with ReflectToString {}