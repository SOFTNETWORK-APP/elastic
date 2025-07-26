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

val testJavaOptions = {
  val heapSize = sys.env.getOrElse("HEAP_SIZE", "1g")
  val extraTestJavaArgs = Seq("-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED").mkString(" ")
  s"-Xmx$heapSize -Xss4m -XX:ReservedCodeCacheSize=128m -Dfile.encoding=UTF-8 $extraTestJavaArgs"
    .split(" ").toSeq
}

Test / javaOptions ++= testJavaOptions

// Required by the Test container framework
Test / fork := true
