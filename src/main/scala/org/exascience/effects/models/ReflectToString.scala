package org.exascience.effects.models

import java.lang.reflect.Field

/**
 * @author Thomas Moerman
 */
trait ReflectToString {

  private def format(field: Field) = {
    val name = field.getName
    val value = field.get(this)

    s"""\"$name\": $value"""
  }

  private def bla(clazz: Class[_ <: _]): String = {
    val fields = clazz.getDeclaredFields

    fields.foreach(field => field setAccessible true)

    fields.map(format).mkString(", ")
  }

  override def toString = bla(getClass.getSuperclass) + ", " + bla(getClass)

}
