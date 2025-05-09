organization := "app.softnetwork.elastic"

name := "elastic-client"

val configDependencies = Seq(
  "com.typesafe" % "config" % Versions.typesafeConfig,
  "com.github.kxbmap" %% "configs" % Versions.kxbmap
)

val jackson = Seq(
  "com.fasterxml.jackson.core"   % "jackson-databind"          % Versions.jackson,
  "com.fasterxml.jackson.core"   % "jackson-core"              % Versions.jackson,
  "com.fasterxml.jackson.core"   % "jackson-annotations"       % Versions.jackson,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % Versions.jackson
)

val jacksonExclusions = Seq(
  ExclusionRule(organization = "com.fasterxml.jackson.core"),
  ExclusionRule(organization = "org.codehaus.jackson")
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext"     % Versions.json4s
).map(_.excludeAll(jacksonExclusions: _*)) ++ jackson

val httpComponentsExclusions = Seq(
  ExclusionRule(
    organization = "org.apache.httpcomponents",
    name = "httpclient",
    artifact = "*",
    configurations = Vector(ConfigRef("test")),
    crossVersion = CrossVersion.disabled
  )
)

val guavaExclusion = ExclusionRule(organization = "com.google.guava", name = "guava")

val jest = Seq(
  "io.searchbox" % "jest" % Versions.jest
).map(_.excludeAll(httpComponentsExclusions ++ Seq(guavaExclusion): _*))

libraryDependencies ++= configDependencies ++ json4s ++ jest :+
  "app.softnetwork.persistence" %% "persistence-core" % Versions.genericPersistence
