name := "435-term-project"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= {

  val sparkVer = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided" withSources(),
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided" withSources(),
    "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" % "provided" withSources()
  )
}