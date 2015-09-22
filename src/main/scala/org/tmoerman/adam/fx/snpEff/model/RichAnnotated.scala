package org.tmoerman.adam.fx.snpeff.model

import org.tmoerman.adam.fx.avro.{AnnotatedGenotype, AnnotatedVariant}

/**
 * @author Thomas Moerman
 */
object RichAnnotated {

  import RichSnpEffAnnotations._

  implicit def pimpAnnotatedVariant(annotatedVariant: AnnotatedVariant): RichAnnotated[AnnotatedVariant] =
    RichAnnotated(annotatedVariant)

  implicit def pimpAnnotatedGenotype(annotatedGenotype: AnnotatedGenotype): RichAnnotated[AnnotatedGenotype] =
    RichAnnotated(annotatedGenotype)
  
  def apply(annotatedVariant: AnnotatedVariant): RichAnnotated[AnnotatedVariant] =
    new RichAnnotated[AnnotatedVariant](annotatedVariant, annotatedVariant.getAnnotations.asOption)
  
  def apply(annotatedGenotype: AnnotatedGenotype): RichAnnotated[AnnotatedGenotype] =
    new RichAnnotated[AnnotatedGenotype](annotatedGenotype, annotatedGenotype.getAnnotations.asOption)
  
}

case class RichAnnotated[A](inner: A, annotations: Option[RichSnpEffAnnotations]) {}
