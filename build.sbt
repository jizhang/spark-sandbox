name := "spark-sandbox"

organization := "com.shzhangji"

version := "0.1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= {
  val sparkVersion = "2.3.2"
  Seq(
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "com.github.scopt" %% "scopt" % "3.3.0",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    "org.scalikejdbc" %% "scalikejdbc" % "3.0.1",
    "mysql" % "mysql-connector-java" % "5.1.43"
  )
}

runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
