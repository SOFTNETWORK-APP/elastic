package app.softnetwork.elastic.client.java

import app.softnetwork.elastic.client.ElasticConfig
import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder}

trait ElasticsearchClientCompanion extends StrictLogging {

  def elasticConfig: ElasticConfig

  private var client: Option[ElasticsearchClient] = None

  lazy val mapper = new ObjectMapper()

  def apply(): ElasticsearchClient = {
    client match {
      case Some(c) => c
      case _ =>
        val credentialsProvider = new BasicCredentialsProvider()
        if (elasticConfig.credentials.username.nonEmpty) {
          credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(
              elasticConfig.credentials.username,
              elasticConfig.credentials.password
            )
          )
        }
        val restClientBuilder: RestClientBuilder = RestClient
          .builder(
            HttpHost.create(elasticConfig.credentials.url)
          )
          .setHttpClientConfigCallback((httpAsyncClientBuilder: HttpAsyncClientBuilder) =>
            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
          )
        val transport = new RestClientTransport(restClientBuilder.build(), new JacksonJsonpMapper())
        val c = new ElasticsearchClient(transport)
        client = Some(c)
        c
    }
  }
}
