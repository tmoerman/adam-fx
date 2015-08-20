package org.exascience.effects

import java.util

import org.apache.spark.Logging
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{VariantContext, SequenceDictionary}
import org.bdgenomics.formats.avro.{Contig, Variant}
import htsjdk.variant.variantcontext.{
Allele,
VariantContext => BroadVariantContext
}
import org.exascience.effects.models.VariantContextWithSnpEffAnnotations
import scala.collection.JavaConversions._
import org.exascience.formats.avro.{FunctionalAnnotation, SnpEffAnnotations}


/**
 * @author Thomas Moerman
 */
object VariantContextConverterForSnpEff {

  private val NON_REF_ALLELE = Allele.create("<NON_REF>", false /* !Reference */ )

}

class VariantContextConverterForSnpEff(val vcc: VariantContextConverter, val dict: Option[SequenceDictionary] = None) extends Serializable with Logging {
  import VariantContextConverterForSnpEff._

  private lazy val contigToRefSeq: Map[String, String] = dict match {
    case Some(d) => d.records.filter(_.refseq.isDefined).map(r => r.name -> r.refseq.get).toMap
    case _       => Map.empty
  }

  private def createContig(vc: BroadVariantContext): Contig = {
    val contigName = contigToRefSeq.getOrElse(vc.getChr, vc.getChr)

    Contig.newBuilder()
      .setContigName(contigName)
      .build()
  }

  private def createADAMVariant(vc: BroadVariantContext, alt: Option[String]): Variant = {
    // VCF CHROM, POS, REF and ALT
    val builder = Variant.newBuilder
      .setContig(createContig(vc))
      .setStart(vc.getStart.toLong - 1 /* ADAM is 0-indexed */ )
      .setEnd(vc.getEnd.toLong /* ADAM is 0-indexed, so the 1-indexed inclusive end becomes exclusive */ )
      .setReferenceAllele(vc.getReference.getBaseString)
    alt.foreach(builder.setAlternateAllele(_))
    builder.build
  }

  private def toAdamVariant(vc: BroadVariantContext): Variant = {
    vc.getAlternateAlleles.toList match {
      case List(NON_REF_ALLELE) => {
        createADAMVariant(vc, None /* No alternate allele */)
      }
      case List(allele) => {
        assert(allele.isNonReference,
          "Assertion failed when converting: " + vc.toString)
        createADAMVariant(vc, Some(allele.getDisplayString))
      }
      case List(allele, NON_REF_ALLELE) => {
        assert(allele.isNonReference,
          "Assertion failed when converting: " + vc.toString)
        createADAMVariant(vc, Some(allele.getDisplayString))
      }
      case alleles :+ NON_REF_ALLELE => {
        throw new scala.IllegalArgumentException("Multi-allelic site with non-ref symbolic allele " +
          vc.toString)
      }
      case _ => {
        throw new scala.IllegalArgumentException("Multi-allelic site " + vc.toString)
      }
    }
  }

  private def extractSnpEffAnnotations(variant: Variant, vc: BroadVariantContext): SnpEffAnnotations = {
    val annotation = SnpEffAnnotations.newBuilder()
      .setVariant(variant)
      .build

    SnpEffAnnotationsParser.convert(vc, annotation)
  }

  def convertToSnpEffAnnotations(vc: BroadVariantContext): SnpEffAnnotations = {
    val variant = toAdamVariant(vc)

    extractSnpEffAnnotations(variant, vc)
  }

  def convertToVariantsWithSnpEffAnnotations(bvc: BroadVariantContext): Seq[VariantContextWithSnpEffAnnotations] = {
    val variantContexts: Seq[VariantContext] = vcc.convert(bvc)

    variantContexts.map(v => VariantContextWithSnpEffAnnotations(v, extractSnpEffAnnotations(v.variant, bvc)))
  }

}