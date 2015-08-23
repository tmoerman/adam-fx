# adam-fx

A Scala library extending [ADAM](https://github.com/bigdatagenomics/adam) and [BDG-formats](https://github.com/bigdatagenomics/bdg-formats) 
to load .vcf files annotated with [SnpEff](http://snpeff.sourceforge.net/). 

## Get the Maven artifact

Artifacts are published to [Bintray](https://bintray.com/tmoerman/maven/adam-fx)

* SBT

    `resolvers += "bintray-tmoerman" at ""http://dl.bintray.com/tmoerman/maven"`

    `libraryDependencies += "org.tmoerman" % "adam-fx_2.10" % "0.1"`

    (substitute version with latest)

## TODO

- saving to Parquet
- loading from Parquet
- tests on large annotated .vcf files
