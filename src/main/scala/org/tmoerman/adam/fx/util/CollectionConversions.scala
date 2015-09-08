package org.tmoerman.adam.fx.util

import java.util.{List => JList}
import scala.collection.JavaConverters._

/**
 * @author Thomas Moerman
 */
object CollectionConversions {

  implicit def immutableScalaList[A](list: JList[A]): List[A] = list.asScala.toList

}
