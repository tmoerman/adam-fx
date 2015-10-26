package org.tmoerman.adam.fx.snpeff

import java.lang.Boolean.TRUE
import java.util.{List => JList}

import htsjdk.variant.variantcontext.{VariantContext => BroadVariantContext}
import htsjdk.variant.vcf.VCFHeaderLineCount._
import htsjdk.variant.vcf.VCFHeaderLineType._
import htsjdk.variant.vcf.{VCFConstants, VCFInfoHeaderLine}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.bdgenomics.adam.converters.AttrKey
import org.tmoerman.adam.fx.avro._
import org.tmoerman.adam.fx.util.ParseFunctions._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * @author Thomas Moerman
  */
object SnpEffAnnotationsParser extends Serializable {

  private def attrAsInt(attr: Object): Object = attr match {
    case a: String            => java.lang.Integer.valueOf(a)
    case a: java.lang.Integer => a
    case a: java.lang.Number  => java.lang.Integer.valueOf(a.intValue)
  }

  def flagParser(attr: Object): Object = TRUE

  def multiIntParser(attr: Object): Object = attr match {
    case s: String        => splitAtPipe(s).map(parseInt).toList.asJava
    case l: JList[String] => l.flatMap(splitAtPipe).map(parseInt).toList.asJava
  }

  def multiStringParser(attr: Object): Object = attr match {
    case s: String        => splitAtPipe(s).toList.asJava
    case l: JList[String] => l.flatMap(splitAtPipe).toList.asJava
  }

  def parseRatio(s: String): Ratio = parseIntPair(s).map{ case (a, b) => new Ratio(a, b) }.orNull

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
    case l: JList[String] => l.map(toFunctionalAnnotation).asJava
  }

  def toEffectPrediction(s: String): EffectPrediction = {
    val attributes = cleanAndSplitAtPipe(s)

    new EffectPrediction(
      attributes(0),
      attributes(1),
      java.lang.Integer.valueOf(attributes(2)),
      java.lang.Float.valueOf(attributes(3)))
  }

  def effectPredictionParser(attr: Object): JList[EffectPrediction] = attr match {
    case s: String        => List(toEffectPrediction(s)).asJava
    case l: JList[String] => l.map(toEffectPrediction).asJava
  }

  val DB_SNP_INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("RS", multiIntParser _, new VCFInfoHeaderLine("RS", 1, String, "RS: dbSNP ID")),

    AttrKey("VLD", flagParser _, new VCFInfoHeaderLine("VLD", 0, Flag, "VLD: is validated")),
    AttrKey("G5A", flagParser _, new VCFInfoHeaderLine("G5A", 0, Flag, "G5A: >5% minor allele frequency")),
    AttrKey("MUT", flagParser _, new VCFInfoHeaderLine("MUT", 0, Flag, "MUT: is mutation")),
    AttrKey("OM",  flagParser _, new VCFInfoHeaderLine("OM",  0, Flag, "OM: has OMIM/OMIA")),
    AttrKey("PM",  flagParser _, new VCFInfoHeaderLine("PM",  0, Flag, "PM: is precious"))
  )

  val CLINVAR_INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("CLNDSDB",   multiStringParser _, new VCFInfoHeaderLine("CLNDSDB",   UNBOUNDED, String, "CLNDSDB: Variant Disease Database Name")),
    AttrKey("CLNACC",    multiStringParser _, new VCFInfoHeaderLine("CLNACC",    UNBOUNDED, String, "CLNACC: Variant Accession and Versions")),
    AttrKey("CLNDBN",    multiStringParser _, new VCFInfoHeaderLine("CLNDBN",    UNBOUNDED, String, "CLNDBN: Variant Disease Name")),
    AttrKey("CLNSRC",    multiStringParser _, new VCFInfoHeaderLine("CLNSRC",    UNBOUNDED, String, "CLNSRC: Variant Clinical Channels")),
    AttrKey("CLNSIG",    multiIntParser _,    new VCFInfoHeaderLine("CLNSIG",    UNBOUNDED, String, "CLNSIG: Variant Clinical Significance")),
    AttrKey("CLNORIGIN", multiStringParser _, new VCFInfoHeaderLine("CLNORIGIN", UNBOUNDED, String, "CLNORIGIN: Allele Origin")),
    AttrKey("CLNDSDBID", multiStringParser _, new VCFInfoHeaderLine("CLNDSDBID", UNBOUNDED, String, "CLNDSDBID: Variant Disease Database ID")),
    AttrKey("CLNHGVS",   multiStringParser _, new VCFInfoHeaderLine("CLNHGVS",   UNBOUNDED, String, "CLNHGVS: Variant Names From HGVS")),
    AttrKey("CLNSRCID",  multiStringParser _, new VCFInfoHeaderLine("CLNSRCID",  UNBOUNDED, String, "CLNSRCID: Variant Clinical Channel IDs"))
  )

  val SNP_EFF_INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("functionalAnnotations",  annParser _,              new VCFInfoHeaderLine("ANN", 1, String, "ANN: functional annotations")),
    AttrKey("lossOfFunction",         effectPredictionParser _, new VCFInfoHeaderLine("LOF", 1, String, "LOF: loss of function")),
    AttrKey("nonsenseMediatedDecay",  effectPredictionParser _, new VCFInfoHeaderLine("NMD", 1, String, "NMD: nonsense mediated decay")))

  lazy val VCF2DbSnpAnnotations: Map[String, (Int, Object => Object)]   = createFieldMap(DB_SNP_INFO_KEYS, DbSnpAnnotations.getClassSchema)

  lazy val VCF2ClinvarAnnotations: Map[String, (Int, Object => Object)] = createFieldMap(CLINVAR_INFO_KEYS, ClinvarAnnotations.getClassSchema)

  lazy val VCF2SnpEffAnnotations: Map[String, (Int, Object => Object)]  = createFieldMap(SNP_EFF_INFO_KEYS, SnpEffAnnotations.getClassSchema)

  private def createFieldMap(keys: Seq[AttrKey], schema: Schema): Map[String, (Int, Object => Object)] = {
    keys.filter(_.attrConverter != null).map(field => {
      val avroField = schema.getField(field.adamKey)
      field.vcfKey -> (avroField.pos, field.attrConverter)
    })(collection.breakOut)
  }

  private def fillRecord[T <% SpecificRecord](fieldMap: Map[String, (Int, Object => Object)], vc: BroadVariantContext, record: T): T = {
    for ((v, a) <- fieldMap) {
      val attr = vc.getAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        record.put(a._1, a._2(attr))
      }
    }
    record
  }

  def fill(broadVariantContext: BroadVariantContext, snpEffAnnotations: SnpEffAnnotations): SnpEffAnnotations = {
    addDbSnpAnnotations(broadVariantContext, snpEffAnnotations)

    addClinvarAnnotations(broadVariantContext, snpEffAnnotations)

    fillRecord(VCF2SnpEffAnnotations, broadVariantContext, snpEffAnnotations)
  }

  private def addDbSnpAnnotations(broadVariantContext: BroadVariantContext, snpEffAnnotations: SnpEffAnnotations): Unit = {
    val dbSnpAnnotations = DbSnpAnnotations.newBuilder().build()

    fillRecord(VCF2DbSnpAnnotations, broadVariantContext, dbSnpAnnotations)

    if (! dbSnpAnnotations.getRS.isEmpty) {
      snpEffAnnotations.setDbSnpAnnotations(dbSnpAnnotations)
    }
  }

  private def addClinvarAnnotations(broadVariantContext: BroadVariantContext, snpEffAnnotations: SnpEffAnnotations): Unit = {
    val clinvarAnnotations = ClinvarAnnotations.newBuilder().build()

    fillRecord(VCF2ClinvarAnnotations, broadVariantContext, clinvarAnnotations)

    if (!clinvarAnnotations.getCLNDSDBID.isEmpty) {
      snpEffAnnotations.setClinvarAnnotations(clinvarAnnotations)
    }
  }

}
