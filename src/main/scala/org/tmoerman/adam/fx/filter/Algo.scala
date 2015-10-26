package org.tmoerman.adam.fx.filter

import org.bdgenomics.adam.models.ReferenceRegion

/**
 * @author Thomas Moerman
 */
object Algo {

  def compact(regions: List[ReferenceRegion]) =
    regions
      .sorted
      .foldLeft(List[ReferenceRegion]())(
        (acc, region) => acc match {
          case Nil     => List(region)
          case x :: xs => if (x.overlaps(region))
                            region.merge(x) :: xs
                          else
                            region :: x :: xs
        }
      )

}
