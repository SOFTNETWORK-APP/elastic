organization := "app.softnetwork.elastic"

name := "elastic-testkit"

val jacksonExclusions = Seq(
  ExclusionRule(organization = "com.fasterxml.jackson.core"),
  ExclusionRule(organization = "com.fasterxml.jackson.dataformat"),
  ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
  ExclusionRule(organization = "com.fasterxml.jackson.module")
)

val elastic = Seq(
  "nl.gn0s1s" %% "elastic4s-core"     % Versions.elastic4s exclude ("org.elasticsearch", "elasticsearch") exclude("org.slf4j", "slf4j-api"),
  "org.elasticsearch"      % "elasticsearch"       % Versions.elasticSearch exclude ("org.apache.logging.log4j", "log4j-api") exclude("org.slf4j", "slf4j-api") excludeAll(jacksonExclusions:_*),
  "nl.gn0s1s" %% "elastic4s-testkit"  % Versions.elastic4s exclude ("org.elasticsearch", "elasticsearch") exclude("org.slf4j", "slf4j-api"),
  "org.apache.logging.log4j" % "log4j-api"         % Versions.log4j,
//  "org.apache.logging.log4j" % "log4j-slf4j-impl"  % Versions.log4j,
  "org.apache.logging.log4j" % "log4j-core"        % Versions.log4j,
  "org.testcontainers"       % "elasticsearch"     % Versions.testContainers excludeAll(jacksonExclusions:_*)
)

libraryDependencies ++= elastic  :+
  "app.softnetwork.persistence" %% "persistence-core-testkit" % Versions.genericPersistence