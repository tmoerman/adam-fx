package org.tmoerman.adam.fx.snpeff

import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.FunctionalAnnotation
import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * @author Thomas Moerman
 */
class SnpEffInspections(val rdd: RDD[VariantContextWithSnpEffAnnotations]) extends Serializable {

  /**
   * @return Returns the distinct and sorted RDD of annotations.
   */
  def distinctAnnotations(): RDD[String] = {
    rdd.flatMap(_.snpEffAnnotations
                 .map(_.functionalAnnotations
                       .flatMap(_.getAnnotations))
                 .getOrElse(Nil))
       .distinct()
       .sortBy(identity)
  }

  def distinctTranscriptBiotypes() = distinctFunctionalAnnotationAttributes(_.getTranscriptBiotype).sortBy(identity)

  def distinctFeatureTypes() = distinctFunctionalAnnotationAttributes(_.getFeatureType).sortBy(identity)

  def distinctFunctionalAnnotationAttributes[A: ClassTag](attribute: FunctionalAnnotation => A): RDD[A] = {
    rdd.flatMap[A](_.snpEffAnnotations
                 .map(_.functionalAnnotations
                       .map(fa => attribute(fa))
                       .filter(_ != null))
                 .getOrElse(Nil))
       .distinct()
  }

}