ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_project"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.postgresql" % "postgresql" % "42.5.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.8" % Provided
)
