organization := "app.softnetwork.elastic"

name := "elastic-rest-client"

val jacksonExclusions = Seq(
  ExclusionRule(organization = "com.fasterxml.jackson.core"),
  ExclusionRule(organization = "com.fasterxml.jackson.dataformat"),
  ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
  ExclusionRule(organization = "com.fasterxml.jackson.module"),
  ExclusionRule(organization = "org.codehaus.jackson")
)

val rest = Seq(
  "org.elasticsearch" % "elasticsearch" % Versions.elasticSearch exclude ("org.apache.logging.log4j", "log4j-api"),
  "co.elastic.clients" % "elasticsearch-java" % Versions.rest exclude ("org.elasticsearch", "elasticsearch"),
  "org.elasticsearch.client" % "elasticsearch-rest-client" % Versions.rest
).map(_.excludeAll(jacksonExclusions: _*))

libraryDependencies ++= rest
