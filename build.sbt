name := "spark-sandbox"

organization := "com.shzhangji"

version := "0.1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.2.0"
  Seq(
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "com.github.scopt" %% "scopt" % "3.3.0",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
  )
}

runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
