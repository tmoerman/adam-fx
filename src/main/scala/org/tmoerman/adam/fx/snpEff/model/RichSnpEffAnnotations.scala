package org.tmoerman.adam.fx.snpeff.model

import org.tmoerman.adam.fx.avro._
import org.tmoerman.adam.fx.util.ReflectToString

/**
 * @author Thomas Moerman
 */
object RichSnpEffAnnotations {

  implicit def pimpAnnotations(snpEffAnnotations: SnpEffAnnotations): RichSnpEffAnnotations =
    new RichSnpEffAnnotations(snpEffAnnotations)

}

case class RichSnpEffAnnotations(inner: SnpEffAnnotations) extends Serializable with ReflectToString {

  import org.tmoerman.adam.fx.util.CollectionConversions.immutableScalaList

  lazy val functionalAnnotations: List[FunctionalAnnotation] = inner.getFunctionalAnnotations

  lazy val lossOfFunction:        List[EffectPrediction]     = inner.getLossOfFunction

  lazy val nonsenseMediatedDecay: List[EffectPrediction]     = inner.getNonsenseMediatedDecay

  lazy val dbSnpAnnotations:      Option[DbSnpAnnotations]   = Option(inner.getDbSnpAnnotations)

  lazy val clinvarAnnotations:    Option[ClinvarAnnotations] = Option(inner.getClinvarAnnotations)

  lazy val isEmpty = functionalAnnotations.isEmpty &&
                     lossOfFunction.isEmpty        &&
                     nonsenseMediatedDecay.isEmpty &&
                     dbSnpAnnotations.isEmpty      &&
                     clinvarAnnotations.isEmpty

  def asOption = if (isEmpty) None else Option(this)

}