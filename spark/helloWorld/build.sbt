name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark"  %% "spark-core"    % sparkVersion
libraryDependencies += "org.apache.spark"  %% "spark-sql"     % sparkVersion
