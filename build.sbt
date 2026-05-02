name := "oss-pulse"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

// Fork JVM for run tasks
fork := true
