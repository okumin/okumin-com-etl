name := "okumin-com-etl"

version := "1.0"

scalaVersion := "2.12.3"

val scioVersion = "0.4.0-beta2"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-bigquery" % scioVersion,
  "com.typesafe.play" % "play-json_2.12" % "2.6.3",
  "com.spotify" %% "scio-test" % scioVersion % "test"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)