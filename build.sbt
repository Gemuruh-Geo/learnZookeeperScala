name := "learnZookeeper"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.7.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}