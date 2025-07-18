package app.softnetwork.elastic.client.java

import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.model.Timestamped

trait ElasticsearchClientProvider[T <: Timestamped]
    extends ElasticProvider[T]
    with ElasticsearchClientApi {
  _: ManifestWrapper[T] =>

}
