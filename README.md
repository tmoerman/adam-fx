# adam-fx

A Scala library extending [ADAM](https://github.com/bigdatagenomics/adam) and [BDG-formats](https://github.com/bigdatagenomics/bdg-formats) 
to load .vcf files annotated with [SnpEff](http://snpeff.sourceforge.net/). 

## Get the Maven artifact

Artifacts are published to [Bintray](https://bintray.com/tmoerman/maven/adam-fx).

##### SBT

```
resolvers += "bintray-tmoerman" at "http://dl.bintray.com/tmoerman/maven"`

libraryDependencies += "org.tmoerman" % "adam-fx_2.10" % "0.2.1"
```

##### Zeppelin

```
%dep

z.addRepo("bintray-tmoerman").url("http://dl.bintray.com/tmoerman/maven")

z.load("org.tmoerman:adam-fx_2.10:0.2.1")
```
    
## Usage

##### Kryo

Adam-fx has its own `KryoRegistrator` that extends the `ADAMKryoRegistrator` with additional Avro data types. Use it
when initializing a `SparkConf`.
      
```scala
val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
    .set("spark.kryo.registrator", "org.tmoerman.adam.fx.serialization.AdamFxKryoRegistrator")
    .set("spark.kryo.referenceTracking", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
val sc = new SparkContext(conf)
```

##### SnpEffContext

Instantiate a `SnpEffContext`, passing it a `SparkContext`. 

In a notebook setting you may want to use the `@transient` annotation in order to prevent serialization issues.

```scala
import org.tmoerman.adam.fx.snpeff.SnpEffContext

@transient val ec = new SnpEffContext(sc)
```
    
##### Loading the SnpEffAnnotations

There are two ways to load the SnpEff annotations from an annotated .vcf file. 

We can either load the raw Avro data structures:

```scala
val annotations: RDD[SnpEffAnnotations] = ec.loadSnpEffAnnotations(annotatedVcf)
```

Or we can load as rich Scala-esque data types, arguably the preferable data types to work with.

```
val variants: RDD[VariantContextWithSnpEffAnnotations] = ec.loadVariantsWithSnpEffAnnotations(annotatedVcf)
```

The methods are capable to load `SnpEffAnnotations` from Parquet storage. The convention is that Parquet files are 
named with suffix `.adam`. We can save an `RDD[SnpEffAnnotations]` to Parquet in the usual Adam way:
 
```
annotations.adamParquetSave("/my/data/dir/annotations.adam")
```

Note that if the `loadVariantsWithSnpEffAnnotations` is used to load data that was previously saved to Parquet, 
the genotypes and databases are not loaded because that data is not part of the raw `SnpEffAnnotations` data type.
