package org.exascience.effects.models

import org.exascience.formats.avro.{NonsenseMediateDecay, LossOfFunction, FunctionalAnnotation, SnpEffAnnotations}
import scala.collection.JavaConverters._

/**
 * @author Thomas Moerman
 */
class RichSnpEffAnnotations(val inner: SnpEffAnnotations) extends Serializable {

  val functionalAnnotations: List[FunctionalAnnotation] = inner.getFunctionalAnnotations.asScala.toList

  val lossOfFunction: Option[LossOfFunction] = Option(inner.getLossOfFunction)

  val nonsenseMediateDecay: Option[NonsenseMediateDecay] = Option(inner.getNonsenseMediateDecay)

  lazy val isEmpty = functionalAnnotations.isEmpty &&
                     lossOfFunction.isEmpty        &&
                     nonsenseMediateDecay.isEmpty

  lazy val asOption = if (isEmpty) None else Option(this)

}