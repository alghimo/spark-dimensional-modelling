name := "spark-dimensional-modelling"
organization := "org.alghimo"
val sparkVersion = "2.1.0"
version := s"spark_${sparkVersion}_0.1.5-SNAPSHOT"
publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

scalaVersion := "2.11.8"
parallelExecution in Test := false
sourcesInBase := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")
target in doc := baseDirectory.value / "docs"
sources in doc := Seq(baseDirectory.value / "src")

val scalaTestVersion             = "3.0.1"
val sparkTestingBaseVersion      = "0.6.0"
val sparkTestingBaseSparkVersion = "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % (s"${sparkTestingBaseSparkVersion}_${sparkTestingBaseVersion}") % "test"
