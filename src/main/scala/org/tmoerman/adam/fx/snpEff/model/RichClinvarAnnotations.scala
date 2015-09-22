package org.tmoerman.adam.fx.snpeff.model

import org.tmoerman.adam.fx.avro.ClinvarAnnotations
import org.tmoerman.adam.fx.util.ReflectToString
import org.tmoerman.adam.fx.util.CollectionConversions.immutableScalaList

/**
 * @author Thomas Moerman
 */
object RichClinvarAnnotations {

  implicit def pimp(a: ClinvarAnnotations): RichClinvarAnnotations = new RichClinvarAnnotations(a)

}

case class RichClinvarAnnotations(inner: ClinvarAnnotations) extends Serializable with ReflectToString {

  def variantDiseaseDatabaseName:  List[String] = inner.getCLNDSDB

  def variantAccessionAndVersions: List[String] = inner.getCLNACC

  def variantDiseaseName:          List[String] = inner.getCLNDBN

  def variantClinicalChannels:     List[String] = inner.getCLNSRC

  def variantClinicalSignificance: List[Int]    = inner.getCLNSIG.map(_.intValue)

  def alleleOrigin:                List[String] = inner.getCLNORIGIN

  def variantDiseaseDatabaseID:    List[String] = inner.getCLNDSDBID

  def variantNamesFromHGVS:        List[String] = inner.getCLNHGVS

  def variantClinicalChannelIDs:   List[String] = inner.getCLNSRCID

}