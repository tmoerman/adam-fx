package org.tmoerman.adam.fx.snpeff.model

import org.tmoerman.adam.fx.avro.DbSnpAnnotations
import org.tmoerman.adam.fx.util.ReflectToString
import org.tmoerman.adam.fx.util.CollectionConversions.immutableScalaList

/**
 * http://varianttools.sourceforge.net/Annotation/DbSNP
 *
 * @author Thomas Moerman
 */
object RichDbSnpAnnotations {

  implicit def pimpDbSnpAnnotations(a: DbSnpAnnotations): RichDbSnpAnnotations = new RichDbSnpAnnotations(a)

}

case class RichDbSnpAnnotations(inner: DbSnpAnnotations) extends Serializable with ReflectToString {

  def dbSnpIDs:                   List[Int] = inner.getRS.map(_.intValue)

  def gt5PctMinorAlleleFrequency: Boolean = inner.getG5A

  def isValidated:                Boolean = inner.getVLD

  def isMutation:                 Boolean = inner.getMUT

  def isPrecious:                 Boolean = inner.getPM

  def hasOmimOmia:                Boolean = inner.getOM

}
