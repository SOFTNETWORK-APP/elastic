organization := "app.softnetwork.elastic"

name := "elastic-jest-client"

val guavaExclusion = ExclusionRule(organization = "com.google.guava", name = "guava")

val httpComponentsExclusions = Seq(
  ExclusionRule(
    organization = "org.apache.httpcomponents",
    name = "httpclient",
    artifact = "*",
    configurations = Vector(ConfigRef("test")),
    crossVersion = CrossVersion.disabled
  )
)

val jest = Seq(
  "io.searchbox" % "jest" % Versions.jest
).map(_.excludeAll(httpComponentsExclusions ++ Seq(guavaExclusion): _*))

libraryDependencies ++= jest
