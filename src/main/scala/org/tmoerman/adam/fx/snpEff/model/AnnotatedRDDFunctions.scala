package org.tmoerman.adam.fx.snpeff.model

import org.apache.spark.rdd.RDD
import org.tmoerman.adam.fx.avro.{AnnotatedGenotype, AnnotatedVariant}

import RichAnnotated._

class AnnotatedVariantRDDFunctions(rdd: RDD[AnnotatedVariant]) extends Serializable {

  def toRichAnnotatedVariants: RDD[RichAnnotatedVariant] = rdd.map(pimpAnnotatedVariant)

}

class AnnotatedGenotypeRDDFunctions(rdd: RDD[AnnotatedGenotype]) extends Serializable {

  def toRichAnnotatedGenotypes: RDD[RichAnnotatedGenotype] = rdd.map(pimpAnnotatedGenotype)

}