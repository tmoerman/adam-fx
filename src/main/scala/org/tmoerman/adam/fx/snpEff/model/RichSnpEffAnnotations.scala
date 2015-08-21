package org.tmoerman.adam.fx.snpEff.model

import org.tmoerman.adam.fx.avro.{FunctionalAnnotation, LossOfFunction, NonsenseMediateDecay, SnpEffAnnotations}
import org.tmoerman.adam.fx.util.ReflectToString

import scala.collection.JavaConverters._

/**
 * @author Thomas Moerman
 */
class RichSnpEffAnnotations(private[this] val inner: SnpEffAnnotations) extends Serializable with ReflectToString {

  val functionalAnnotations: List[FunctionalAnnotation] = inner.getFunctionalAnnotations.asScala.toList

  val lossOfFunction: Option[LossOfFunction] = Option(inner.getLossOfFunction)

  val nonsenseMediateDecay: Option[NonsenseMediateDecay] = Option(inner.getNonsenseMediateDecay)

  def isEmpty() = functionalAnnotations.isEmpty &&
                  lossOfFunction.isEmpty        &&
                  nonsenseMediateDecay.isEmpty

  def asOption() = if (isEmpty()) None else Option(this)

}