package org.tmoerman.adam.fx.snpeff.model

import org.tmoerman.adam.fx.avro._
import org.tmoerman.adam.fx.util.ReflectToString

import scala.collection.JavaConverters._

/**
 * @author Thomas Moerman
 */
case class RichSnpEffAnnotations(inner: SnpEffAnnotations) extends Serializable with ReflectToString {

  val functionalAnnotations: List[FunctionalAnnotation] = inner.getFunctionalAnnotations.asScala.toList

  val lossOfFunction: Option[EffectPrediction] = Option(inner.getLossOfFunction)

  val nonsenseMediatedDecay: Option[EffectPrediction] = Option(inner.getNonsenseMediatedDecay)

  lazy val isEmpty = functionalAnnotations.isEmpty &&
                     lossOfFunction.isEmpty        &&
                     nonsenseMediatedDecay.isEmpty

  def asOption() = if (isEmpty) None else Option(this)

}