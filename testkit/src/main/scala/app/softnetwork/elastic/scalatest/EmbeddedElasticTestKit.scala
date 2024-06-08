package app.softnetwork.elastic.scalatest

import org.scalatest.Suite
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic
import pl.allegro.tech.embeddedelasticsearch.PopularProperties._

import java.net.ServerSocket
import java.util.concurrent.TimeUnit

trait EmbeddedElasticTestKit extends ElasticTestKit { _: Suite =>

  override lazy val elasticURL: String = s"http://127.0.0.1:${embeddedElastic.getHttpPort}"

  override def stop(): Unit = embeddedElastic.stop()

  private[this] def dynamicPort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  private[this] val embeddedElastic: EmbeddedElastic = EmbeddedElastic
    .builder()
    .withElasticVersion(elasticVersion)
    .withSetting(HTTP_PORT, dynamicPort)
    .withSetting(CLUSTER_NAME, clusterName)
    .withCleanInstallationDirectoryOnStop(true)
    .withEsJavaOpts("-Xms128m -Xmx512m")
    .withStartTimeout(2, TimeUnit.MINUTES)
    .build()
    .start()

}
