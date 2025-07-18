package app.softnetwork.elastic.scalatest

import org.scalatest.Suite
import org.testcontainers.containers.BindMode
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}

/** Created by smanciot on 28/06/2018.
 */
trait ElasticDockerTestKit extends ElasticTestKit { _: Suite =>

  override lazy val elasticURL: String = s"http://${elasticContainer.getHttpHostAddress}"

  lazy val tmpDir: Path = Files.createTempDirectory("es-tmp")

  lazy val elasticContainer: ElasticsearchContainer = {
    val container = new ElasticsearchContainer(
      DockerImageName
        .parse(s"docker.elastic.co/elasticsearch/elasticsearch")
        .withTag(elasticVersion)
    )
    container.addEnv("ES_TMPDIR", "/tmp")
    container.addEnv("discovery.type", "single-node")
    container.addEnv("xpack.security.enabled", "false")
    container.addEnv("xpack.ml.enabled", "false")
    container.addEnv("xpack.watcher.enabled", "false")
    container.addEnv("xpack.graph.enabled", "false")
    container.addFileSystemBind(tmpDir.toAbsolutePath.toString, "/tmp", BindMode.READ_WRITE)
    container
  }

  override def start(): Unit = elasticContainer.start()

  override def stop(): Unit = elasticContainer.stop()

}
