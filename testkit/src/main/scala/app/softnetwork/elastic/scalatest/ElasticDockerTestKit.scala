package app.softnetwork.elastic.scalatest

import org.scalatest.Suite
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

/** Created by smanciot on 28/06/2018.
  */
trait ElasticDockerTestKit extends ElasticTestKit { _: Suite =>

  override lazy val elasticURL: String = s"http://${elasticContainer.getHttpHostAddress}"

  lazy val elasticContainer = new ElasticsearchContainer(
    DockerImageName
      .parse(s"elasticsearch:$elasticVersion")
      .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch")
  )

  override def start(): Unit = elasticContainer.start()

  override def stop(): Unit = elasticContainer.stop()

}
