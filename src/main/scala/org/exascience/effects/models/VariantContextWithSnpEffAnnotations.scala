package org.exascience.effects.models

import org.bdgenomics.adam.models.{VariantContext, ReferencePosition}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.{DatabaseVariantAnnotation, Genotype}
import org.exascience.formats.avro.SnpEffAnnotations

/**
 * Extends the Adam VariantContext type with optional SnpEffAnnotations.
 *
 * @author Thomas Moerman
 */
object VariantContextWithSnpEffAnnotations {

  def apply(variantContext: VariantContext, snpEffAnnotations: SnpEffAnnotations) =
    new VariantContextWithSnpEffAnnotations(
      variantContext.position,
      variantContext.variant,
      variantContext.genotypes,
      variantContext.databases,
      new RichSnpEffAnnotations(snpEffAnnotations).asOption)

}

class VariantContextWithSnpEffAnnotations(position: ReferencePosition,
                                          variant: RichVariant,
                                          genotypes: Iterable[Genotype],
                                          databases: Option[DatabaseVariantAnnotation] = None,
                                          val snpEffAnnotations: Option[RichSnpEffAnnotations] = None)
  extends VariantContext(position, variant, genotypes, databases) {}