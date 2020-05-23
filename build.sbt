name := "zio-playground"

version := "0.1"

scalaVersion := "2.12.11"

lazy val zioVersion = "1.0.0-RC19"
lazy val sparkVersion = "2.4.5"
lazy val deltaVersion = "0.6.0"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % zioVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.delta" %% "delta-core" % deltaVersion
)
