Spark Sandbox
=============

[![Build Status](https://travis-ci.org/jizhang/spark-sandbox.svg?branch=master)](https://travis-ci.org/jizhang/spark-sandbox)

## Install sbt

* Download [sbt-launch.jar][1], and put it into $HOME/bin.
* Create $HOME/bin/sbt, and change mode to 755. The content is:

```bash
SBT_OPTS="-Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256M -Dsbt.override.build.repos=true"
java $SBT_OPTS -jar `dirname $0`/sbt-launch.jar "$@"
```

* Create $HOME/.sbt/repositories, content is:

```
[repositories]
  local
  my-ivy-proxy-releases: http://10.20.8.31:8081/nexus/content/groups/ivy-releases/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]
  my-maven-proxy-releases: http://10.20.8.31:8081/nexus/content/groups/public/
```

## Install Spark

* Download [Spark][2], choose the version corresponding to your HDFS.
* Extract the tar ball, say /path/to/spark
* Setup $SPARK_HOME=/path/to/spark
* Add $SPARK_HOME/bin to $PATH

## Import Project

```bash
$ git clone git@github.com:jizhang/spark-sandbox
$ cd spark-sandbox
$ sbt eclipse
```

And import the project into Eclipse, provided [ScalaIDE for Eclipse][3] is installed.

## Wordcount

```bash
$ cd spark-sandbox
$ sbt package
$ spark-submit --class Wordcount --master local target/scala-2.10/spark-sandbox_2.10-0.1.0.jar data/wordcount.txt
```

## Logistic Regression

```bash
$ spark-submit --class LogisticRegression --master local target/scala-2.10/spark-sandbox_2.10-0.1.0.jar data/lr_data.txt 10 10
```

[1]: https://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.6/sbt-launch.jar
[2]: http://spark.apache.org/downloads.html
[3]: http://scala-ide.org/
