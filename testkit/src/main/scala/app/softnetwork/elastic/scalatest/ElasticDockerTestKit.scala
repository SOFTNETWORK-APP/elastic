package app.softnetwork.elastic.scalatest

import org.scalatest.Suite
import org.testcontainers.elasticsearch.ElasticsearchContainer

import scala.util.{Failure, Success}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticDockerTestKit extends ElasticTestKit { _: Suite =>

  override lazy val elasticURL: String = s"http://${elasticContainer.getHttpHostAddress}"

  lazy val elasticContainer = new ElasticsearchContainer(
    s"docker.elastic.co/elasticsearch/elasticsearch:$elasticVersion"
  )

  override def start(): Unit = elasticContainer.start()

  override def stop(): Unit = elasticContainer.stop()

}
