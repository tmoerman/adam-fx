# adam-fx

A Scala library extending [ADAM](https://github.com/bigdatagenomics/adam) and [BDG-formats](https://github.com/bigdatagenomics/bdg-formats) to load .vcf files annotated with [SnpEff](http://snpeff.sourceforge.net/). 

*[WARNING: this library is still under heavy development. Expect versions to break compatibility.]*

## Get the Maven artifact

Artifacts are published to [Bintray](https://bintray.com/tmoerman/maven/adam-fx).

##### SBT

```sbt
resolvers += "bintray-tmoerman" at "http://dl.bintray.com/tmoerman/maven"`

libraryDependencies += "org.tmoerman" %% "adam-fx" % "0.5.5"
```

##### Spark Notebook

```
:remote-repo bintray-tmoerman % default % http://dl.bintray.com/tmoerman/maven % maven

:dp org.tmoerman %% adam-fx % 0.5.5
```

##### Zeppelin

```
%dep

z.addRepo("bintray-tmoerman").url("http://dl.bintray.com/tmoerman/maven")

z.load("org.tmoerman:adam-fx_2.10:0.5.5")
```

## Data model

The `AnnotatedVariant` and `AnnotatedGenotype` classes are the "connector" types between the Adam types and the SnpEffAnnotations.

Class diagrams distilled from the Java classes generated from the Avro schema definition. 

##### Overview:

![Class diagram](img/adam_fx_small_diagram.png?raw=true)

##### With properties:

![Class diagram](img/adam_fx_class_diagram.png?raw=true)

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

Or you could simply import the implicit conversions and use an (already instantiated) SparkContext reference.

```scala
import org.tmoerman.adam.fx.snpeff.SnpEffContext._
```    
    
##### Loading data

Loading Variants with SnpEffAnnotations:

```scala
val annotatedVariants: RDD[AnnotatedVariant] = sc.loadAnnotatedVariants(annotatedVcf)
```

Or Genotypes with SnpEffAnnotations:

```scala
val annotatedGenotypes: RDD[AnnotatedGenotype] = sc.loadAnnotatedGenotypes(annotatedVcf)
```
