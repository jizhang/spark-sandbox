name := "spark-sandbox"

organization := "com.shzhangji"

version := "0.1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= {
  val sparkVersion = "1.4.0"
  Seq(
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion intransitive(),
    ("org.apache.kafka" %% "kafka" % "0.8.1.1").
      exclude("com.sun.jmx", "jmxri").
      exclude("com.sun.jdmk", "jmxtools").
      exclude("net.sf.jopt-simple", "jopt-simple").
      exclude("org.slf4j", "slf4j-simple").
      exclude("org.apache.zookeeper", "zookeeper"),
    "org.apache.hbase" % "hbase-common" % "0.98.1-cdh5.1.0" % "provided",
    "org.apache.hbase" % "hbase-client" % "0.98.1-cdh5.1.0" % "provided"
  )
}

runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
