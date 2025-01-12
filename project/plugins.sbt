logLevel := Level.Warn

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

resolvers += "Softnetwork releases" at "https://softnetwork.jfrog.io/artifactory/releases/"

addSbtPlugin("app.softnetwork.sbt-softnetwork" % "sbt-softnetwork-git" % "0.1.7")

addSbtPlugin("app.softnetwork.sbt-softnetwork" % "sbt-softnetwork-info" % "0.1.7")

addSbtPlugin("app.softnetwork.sbt-softnetwork" % "sbt-softnetwork-publish" % "0.1.7")

addDependencyTreePlugin

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.8")
