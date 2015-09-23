package org.tmoerman.adam.fx.snpeff.model

import org.bdgenomics.formats.avro.{Genotype, Variant}
import org.tmoerman.adam.fx.avro.{AnnotatedGenotype, AnnotatedVariant}

/**
 * @author Thomas Moerman
 */
object RichAnnotated {

  import RichSnpEffAnnotations._

  implicit def pimpAnnotatedVariant(annotatedVariant: AnnotatedVariant): RichAnnotatedVariant =
    new RichAnnotatedVariant(annotatedVariant.getVariant, annotatedVariant.getAnnotations.asOption)

  implicit def pimpAnnotatedGenotype(annotatedGenotype: AnnotatedGenotype): RichAnnotatedGenotype =
    new RichAnnotatedGenotype(annotatedGenotype.getGenotype, annotatedGenotype.getAnnotations.asOption)
  
}

case class RichAnnotatedVariant(variant: Variant, annotations: Option[RichSnpEffAnnotations]) {}

case class RichAnnotatedGenotype(genotype: Genotype, annotations: Option[RichSnpEffAnnotations]) {}