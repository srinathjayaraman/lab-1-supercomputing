name := "Lab 1"
version := "1.0"
scalaVersion := "2.12.12"

fork in run := true

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
