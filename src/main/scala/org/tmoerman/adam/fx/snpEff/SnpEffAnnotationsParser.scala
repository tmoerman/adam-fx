package org.tmoerman.adam.fx.snpeff

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.{VCFConstants, VCFHeaderLineType, VCFInfoHeaderLine}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.lang.StringUtils._
import org.bdgenomics.adam.converters.AttrKey
import org.tmoerman.adam.fx.avro._
import java.util.{List => JList}
import scala.collection.JavaConverters._

/**
  * @author Thomas Moerman
  */
object SnpEffAnnotationsParser extends Serializable {

   private val allBetweenBracketsRegex = "\\((.*?)\\)".r

   def removeParentheses(s: String): String = allBetweenBracketsRegex.findFirstMatchIn(s).map(_.group(1)).getOrElse(s)

   def splitAtPipe(s: String): Array[String] = s"$s ".split("\\|").map(s => if (isBlank(s)) null else s.trim)

   def splitAtAmpersand(s: String): Array[String] = s.split("\\&")

   def cleanAndSplitAtPipe = removeParentheses _ andThen splitAtPipe

   def parseInt(s: String) = if (isNotEmpty(s)) Integer.valueOf(s) else null

   def parseIntPair(s: String): Option[(Integer, Integer)] =
     if (isNotEmpty(s)) {
       s.split("/") match {
         case Array(a, b) => Some((parseInt(a), parseInt(b)))
       }
     } else None

   def parseRatio(s: String) = parseIntPair(s).map{ case (a, b) => new Ratio(a, b) }.orNull

   def toFunctionalAnnotation(s: String): FunctionalAnnotation = {
     val attributes = splitAtPipe(s)

     val result = attributes match {

       case Array(allele,    annotation,  impact,    geneName,
                  geneID,    featureType, featureID, transcriptBioType,
                  rank,      hgsvC,       hgsvP,     cdnaPosLen,
                  cdsPosLen, protPosLen,  distance,  errorsWarningsInfo
       ) =>
         new FunctionalAnnotation(
         allele,
         splitAtAmpersand(annotation).toList.asJava,
         Impact.valueOf(impact.toUpperCase),
         geneName,
         geneID,
         featureType,
         featureID,
         transcriptBioType,
         parseRatio(rank),
         hgsvC,
         hgsvP,
         parseRatio(cdnaPosLen),
         parseRatio(cdsPosLen),
         parseRatio(protPosLen),
         distance,
         errorsWarningsInfo)

     }

     result
   }

   def annParser(attr: Object): JList[FunctionalAnnotation] = attr match {
     case s: String        => List(toFunctionalAnnotation(s)).asJava
     case l: JList[String] => l.asScala.map(toFunctionalAnnotation).asJava
   }

   def toLossOfFunction(s: String): LossOfFunction = {
     val attributes = cleanAndSplitAtPipe(s)

     new LossOfFunction(
       attributes(0),
       attributes(1),
       java.lang.Integer.valueOf(attributes(2)),
       java.lang.Float.valueOf(attributes(3)))
   }

   def lofParser(attr: Object): LossOfFunction = attr match {
     case s: String => toLossOfFunction(s)
   }

   def toNonsenseMediateDecay(s: String): NonsenseMediateDecay = {
     val attributes = cleanAndSplitAtPipe(s)

     new NonsenseMediateDecay(
       attributes(0),
       attributes(1),
       java.lang.Integer.valueOf(attributes(2)),
       java.lang.Float.valueOf(attributes(3)))
   }

   def nmdParser(attr: Object): NonsenseMediateDecay = attr match {
     case s: String => toNonsenseMediateDecay(s)
   }

   val SNP_EFF_INFO_KEYS: Seq[AttrKey] = Seq(
     AttrKey("functionalAnnotations", annParser _, new VCFInfoHeaderLine("ANN", 1, VCFHeaderLineType.String, "ANN INFO field: functional annotations")),
     AttrKey("lossOfFunction",        lofParser _, new VCFInfoHeaderLine("LOF", 1, VCFHeaderLineType.String, "LOF INFO field: loss of function")),
     AttrKey("nonsenseMediateDecay",  nmdParser _, new VCFInfoHeaderLine("NMD", 1, VCFHeaderLineType.String, "NMD INFO field: nonsense mediate decay")))

   lazy val VCF2SnpEffAnnotations: Map[String, (Int, Object => Object)] =
     createFieldMap(SNP_EFF_INFO_KEYS, SnpEffAnnotations.getClassSchema)

   private def createFieldMap(keys: Seq[AttrKey], schema: Schema): Map[String, (Int, Object => Object)] = {
     keys.filter(_.attrConverter != null).map(field => {
       val avroField = schema.getField(field.adamKey)
       field.vcfKey -> (avroField.pos, field.attrConverter)
     })(collection.breakOut)
   }

   private def fillRecord[T <% SpecificRecord](fieldMap: Map[String, (Int, Object => Object)], vc: VariantContext, record: T): T = {
     for ((v, a) <- fieldMap) {
       val attr = vc.getAttribute(v)
       if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
         record.put(a._1, a._2(attr))
       }
     }
     record
   }

   def convert(vc: VariantContext, annotation: SnpEffAnnotations): SnpEffAnnotations =
     fillRecord(VCF2SnpEffAnnotations, vc, annotation)

 }
