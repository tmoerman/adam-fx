package org.exascience.effects.serialization

import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.serialization.{AvroSerializer, ADAMKryoRegistrator}
import org.exascience.formats.avro.{NonsenseMediateDecay, FunctionalAnnotation, LossOfFunction, SnpEffAnnotations}

/**
 * @author Thomas Moerman
 */
class AdamEffectsKryoRegistrator extends ADAMKryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    super.registerClasses(kryo)

    kryo.register(classOf[SnpEffAnnotations],    new AvroSerializer[SnpEffAnnotations]())
    kryo.register(classOf[FunctionalAnnotation], new AvroSerializer[FunctionalAnnotation]())
    kryo.register(classOf[LossOfFunction],       new AvroSerializer[LossOfFunction]())
    kryo.register(classOf[NonsenseMediateDecay], new AvroSerializer[NonsenseMediateDecay]())
  }
}
