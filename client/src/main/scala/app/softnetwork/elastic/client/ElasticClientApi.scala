package app.softnetwork.elastic.client

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import akka.NotUsed
import akka.actor.ActorSystem
import _root_.akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl._
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization._
import app.softnetwork.elastic.sql.SQLQuery
import com.google.gson.JsonParser
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticClientApi
    extends IndicesApi
    with SettingsApi
    with AliasApi
    with MappingApi
    with CountApi
    with SingleValueAggregateApi
    with SearchApi
    with IndexApi
    with UpdateApi
    with GetApi
    with BulkApi
    with DeleteApi
    with RefreshApi
    with FlushApi {

  def config: Config = ConfigFactory.load()

  final lazy val elasticConfig: ElasticConfig = ElasticConfig(config)
}

trait IndicesApi {

  /** Default settings for indices. This is used when creating an index without providing specific
    * settings. It includes ngram tokenizer and analyzer, as well as some default limits.
    */
  val defaultSettings: String =
    """
      |{
      |  "index": {
      |    "max_ngram_diff": "20",
      |    "mapping" : {
      |      "total_fields" : {
      |        "limit" : "2000"
      |      }
      |    },
      |    "analysis": {
      |      "analyzer": {
      |        "ngram_analyzer": {
      |          "tokenizer": "ngram_tokenizer",
      |          "filter": [
      |            "lowercase",
      |            "asciifolding"
      |          ]
      |        },
      |        "search_analyzer": {
      |          "type": "custom",
      |          "tokenizer": "standard",
      |          "filter": [
      |            "lowercase",
      |            "asciifolding"
      |          ]
      |        }
      |      },
      |      "tokenizer": {
      |        "ngram_tokenizer": {
      |          "type": "ngram",
      |          "min_gram": 1,
      |          "max_gram": 20,
      |          "token_chars": [
      |            "letter",
      |            "digit"
      |          ]
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin

  /** Create an index with the provided name and settings.
    * @param index
    *   - the name of the index to create
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the index was created successfully, false otherwise
    */
  def createIndex(index: String, settings: String = defaultSettings): Boolean

  /** Delete an index with the provided name.
    * @param index
    *   - the name of the index to delete
    * @return
    *   true if the index was deleted successfully, false otherwise
    */
  def deleteIndex(index: String): Boolean

  /** Close an index with the provided name.
    * @param index
    *   - the name of the index to close
    * @return
    *   true if the index was closed successfully, false otherwise
    */
  def closeIndex(index: String): Boolean

  /** Open an index with the provided name.
    * @param index
    *   - the name of the index to open
    * @return
    *   true if the index was opened successfully, false otherwise
    */
  def openIndex(index: String): Boolean

  /** Reindex from source index to target index.
    * @param sourceIndex
    *   - the name of the source index
    * @param targetIndex
    *   - the name of the target index
    * @return
    *   true if the reindexing was successful, false otherwise
    */
  def reindex(sourceIndex: String, targetIndex: String): Boolean

  /** Check if an index exists.
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the index exists, false otherwise
    */
  def indexExists(index: String): Boolean
}

trait AliasApi {

  /** Add an alias to the given index.
    * @param index
    *   - the name of the index
    * @param alias
    *   - the name of the alias
    * @return
    *   true if the alias was added successfully, false otherwise
    */
  def addAlias(index: String, alias: String): Boolean

  /** Remove an alias from the given index.
    * @param index
    *   - the name of the index
    * @param alias
    *   - the name of the alias
    * @return
    *   true if the alias was removed successfully, false otherwise
    */
  def removeAlias(index: String, alias: String): Boolean
}

trait SettingsApi { _: IndicesApi =>

  /** Toggle the refresh interval of an index.
    * @param index
    *   - the name of the index
    * @param enable
    *   - true to enable the refresh interval, false to disable it
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def toggleRefresh(index: String, enable: Boolean): Boolean = {
    updateSettings(
      index,
      if (!enable) """{"index" : {"refresh_interval" : -1} }"""
      else """{"index" : {"refresh_interval" : "1s"} }"""
    )
  }

  /** Set the number of replicas for an index.
    * @param index
    *   - the name of the index
    * @param replicas
    *   - the number of replicas to set
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def setReplicas(index: String, replicas: Int): Boolean = {
    updateSettings(index, s"""{"index" : {"number_of_replicas" : $replicas} }""")
  }

  /** Update the settings of an index.
    * @param index
    *   - the name of the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def updateSettings(index: String, settings: String = defaultSettings): Boolean

  /** Load the settings of an index.
    * @return
    *   the settings of the index as a JSON string
    */
  def loadSettings(): String
}

trait MappingApi extends IndicesApi with RefreshApi with StrictLogging {
  @deprecated("Use setMapping(index: String, mapping: String) instead", "7.17.29")
  def setMapping(index: String, indexType: String, mapping: String): Boolean = {
    this.setMapping(index, mapping)
  }

  /** Set the mapping of an index.
    * @param index
    *   - the name of the index to set the mapping for
    * @param mapping
    *   - the mapping to set on the index
    * @return
    *   true if the mapping was set successfully, false otherwise
    */
  def setMapping(index: String, mapping: String): Boolean
  @deprecated("Use getMapping(index: String) instead", "7.17.29")
  def getMapping(index: String, indexType: String): String = {
    this.getMapping(index)
  }

  /** Get the mapping of an index.
    * @param index
    *   - the name of the index to get the mapping for
    * @return
    *   the mapping of the index as a JSON string
    */
  def getMapping(index: String): String

  /** Get the mapping properties of an index.
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  def getMappingProperties(index: String): String = {
    val mapping = getMapping(index)
    Try(
      new JsonParser()
        .parse(mapping)
        .getAsJsonObject
        .get(index)
        .getAsJsonObject
        .get("mappings")
        .toString
    ) match {
      case Success(properties) => properties
      case Failure(exception) =>
        logger.error(s"Failed to get mapping properties for index $index", exception)
        "{\"properties\": {}}" // Return an empty properties object in case of failure
    }
  }

  /** Check if the mapping of an index is different from the provided mapping.
    * @param index
    *   - the name of the index to check
    * @param mapping
    *   - the mapping to compare with the current mapping of the index
    * @return
    *   true if the mapping is different, false otherwise
    */
  def shouldUpdateMapping(
    index: String,
    mapping: String
  ): Boolean = {
    MappingComparator.isMappingDifferent(this.getMappingProperties(index), mapping)
  }

  /** Update the mapping of an index to a new mapping.
    * @param index
    *   - the name of the index to migrate
    * @param mapping
    *   - the new mapping to set on the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the mapping was updated successfully, false otherwise
    */
  def updateMapping(
    index: String,
    mapping: String,
    settings: String = defaultSettings
  ): Boolean = {
    // Check if the index exists
    if (!this.indexExists(index)) {
      if (!this.createIndex(index, settings)) {
        logger.error(s"Failed to create index: $index")
        return false
      }
      logger.info(s"Index $index created successfully.")
      if (!this.setMapping(index, mapping)) {
        logger.error(s"Failed to set mapping for index: $index")
        return false
      }
      logger.info(s"Mapping for index $index set successfully.")
      if (!this.refresh(index)) {
        logger.error(s"Failed to refresh index: $index")
        return false
      }
      logger.info(s"Index $index refreshed successfully.")
      true
    }
    // Check if the mapping needs to be updated
    else if (shouldUpdateMapping(index, mapping)) {
      val tempIndex = index + "_tmp_" + UUID.randomUUID()
      var tempCreated = false
      var originalDeleted = false
      logger.info("--- Starting dynamic mapping migration ---")
      logger.info("Target index: " + index)
      logger.info("Temporary index: " + tempIndex)
      def migrate(): Boolean = {
        // Create a temporary index with the new mapping
        tempCreated = this.createIndex(tempIndex, settings)
        if (tempCreated) {
          logger.info(s"Temporary index $tempIndex created successfully.")
          // Set the new mapping on the temporary index
          if (!this.setMapping(tempIndex, mapping)) {
            logger.error(s"Failed to set mapping for temporary index: $tempIndex")
            return false
          }
          logger.info(s"Mapping for temporary index $tempIndex set successfully.")
          // Reindex from the original index to the temporary index
          if (!this.reindex(index, tempIndex)) {
            logger.error(
              s"Failed to reindex from original index: $index to temporary index: $tempIndex"
            )
            return false
          }
          logger.info(
            s"Reindexing from original index $index to temporary index $tempIndex completed successfully."
          )
          // Refresh the temporary index to make sure all documents are indexed
          if (!this.refresh(tempIndex)) {
            logger.error(s"Failed to refresh temporary index: $tempIndex")
            return false
          }
          logger.info(s"Temporary index $tempIndex refreshed successfully.")
          // Close the original index
          this.closeIndex(index)
          // Delete the original index
          originalDeleted = this.deleteIndex(index)
          if (originalDeleted) {
            logger.info(s"Original index $index deleted successfully.")
            // Rename the temporary index to the original index name
            if (!this.createIndex(index, settings)) {
              logger.error(s"Failed to recreate original index: $index")
              return false
            }
            logger.info(s"Original index $index recreated successfully.")
            if (!this.setMapping(index, mapping)) {
              logger.error(s"Failed to set mapping for original index: $index")
              return false
            }
            logger.info(s"Mapping for original index $index set successfully.")
            if (!this.reindex(tempIndex, index)) {
              logger.error(
                s"Failed to reindex from temporary index: $tempIndex to original index: $index"
              )
              return false
            }
            logger.info(
              s"Reindexing from temporary index $tempIndex to original index $index completed successfully."
            )
            if (!this.refresh(index)) {
              logger.error(s"Failed to refresh original index: $index")
              return false
            }
            logger.info(s"Original index $index refreshed successfully.")
            if (!this.openIndex(index)) {
              logger.error(s"Failed to open original index: $index")
              return false
            }
            logger.info(s"Original index $index opened successfully.")
            logger.info("Dynamic mapping migration completed successfully.")
            true
          } else {
            logger.error(s"Failed to delete original index: $index")
            false
          }
        } else {
          logger.error(s"Failed to create temporary index: $tempIndex")
          false
        }
      }
      val migration = Try(migrate()) match {
        case Success(result) => result
        case Failure(exception) =>
          logger.error("Exception during dynamic mapping migration", exception)
          false
      }
      if (!migration) {
        logger.error("Error during dynamic mapping migration")
        if (originalDeleted) {
          // If the original index was deleted, we need to recreate it
          if (!this.createIndex(index, settings)) {
            logger.error(s"Failed to recreate original index: $index")
          } else {
            logger.info(s"Original index $index recreated successfully.")
            // Set the original mapping back
            if (!this.setMapping(index, mapping)) {
              logger.error(s"Failed to set mapping for original index: $index")
            } else {
              logger.info(s"Mapping for original index $index set successfully.")
              if (!this.reindex(tempIndex, index)) {
                logger.error(
                  s"Failed to reindex from temporary index $tempIndex to original index $index"
                )
              } else {
                logger.info(
                  s"Reindexing from temporary index $tempIndex to original index $index completed successfully."
                )
                if (!this.refresh(index)) {
                  logger.error(s"Failed to refresh original index: $index")
                } else {
                  logger.info(s"Original index $index refreshed successfully.")
                }
              }
            }
          }
        }
      }
      if (tempCreated) {
        // Clean up the temporary index if it was created
        if (!this.deleteIndex(tempIndex)) {
          logger.error(s"Failed to delete temporary index: $tempIndex")
        } else {
          logger.info(s"Temporary index $tempIndex deleted successfully.")
        }
      } else {
        logger.error(s"Temporary index $tempIndex was not created, skipping deletion.")
      }
      migration
    } else {
      false
    }
  }
}

trait RefreshApi {

  /** Refresh the index to make sure all documents are indexed and searchable.
    * @param index
    *   - the name of the index to refresh
    * @return
    *   true if the index was refreshed successfully, false otherwise
    */
  def refresh(index: String): Boolean
}

trait FlushApi {

  /** Flush the index to make sure all operations are written to disk.
    * @param index
    *   - the name of the index to flush
    * @param force
    *   - true to force the flush, false otherwise
    * @param wait
    *   - true to wait for the flush to complete, false otherwise
    * @return
    *   true if the index was flushed successfully, false otherwise
    */
  def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean
}

trait IndexApi { _: RefreshApi =>

  /** Index an entity in the given index.
    * @param entity
    *   - the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  def index[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this.index(
      index.getOrElse(indexType),
      entity.uuid,
      serialization.write[U](entity)
    )
  }

  @deprecated("Use index(index: String, id: String, source: String) instead", "7.17.29")
  def index(index: String, indexType: String, id: String, source: String): Boolean = {
    this.index(index, id, source)
  }

  /** Index an entity in the given index.
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  def index(index: String, id: String, source: String): Boolean

  /** Index an entity in the given index asynchronously.
    * @param entity
    *   - the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  def indexAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], ec: ExecutionContext, formats: Formats): Future[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    indexAsync(index.getOrElse(indexType), entity.uuid, serialization.write[U](entity))
  }

  @deprecated("Use indexAsync(index: String, id: String, source: String) instead", "7.17.29")
  def indexAsync(index: String, indexType: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    this.indexAsync(index, id, source)
  }

  /** Index an entity in the given index asynchronously.
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    Future {
      this.index(index, id, source)
    }
  }
}

trait UpdateApi { _: RefreshApi =>

  /** Update an entity in the given index.
    * @param entity
    *   - the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  def update[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this.update(
      index.getOrElse(indexType),
      entity.uuid,
      serialization.write[U](entity),
      upsert
    )
  }

  @deprecated(
    "Use update(index: String, id: String, source: String, upsert: Boolean) instead",
    "7.17.29"
  )
  def update(
    index: String,
    indexType: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    this.update(index, id, source, upsert)
  }

  /** Update an entity in the given index.
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  def update(index: String, id: String, source: String, upsert: Boolean): Boolean

  /** Update an entity in the given index asynchronously.
    * @param entity
    *   - the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  def updateAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], ec: ExecutionContext, formats: Formats): Future[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this
      .updateAsync(
        index.getOrElse(indexType),
        entity.uuid,
        serialization.write[U](entity),
        upsert
      )
  }

  @deprecated(
    "Use updateAsync(index: String, id: String, source: String, upsert: Boolean) instead",
    "7.17.29"
  )
  def updateAsync(index: String, indexType: String, id: String, source: String, upsert: Boolean)(
    implicit ec: ExecutionContext
  ): Future[Boolean] = this.updateAsync(index, id, source, upsert)

  /** Update an entity in the given index asynchronously.
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    Future {
      this.update(index, id, source, upsert)
    }
  }
}

trait DeleteApi { _: RefreshApi =>

  /** Delete an entity from the given index.
    * @param entity
    *   - the entity to delete
    * @param index
    *   - the name of the index to delete the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   true if the entity was deleted successfully, false otherwise
    */
  def delete[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U]): Boolean = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    delete(entity.uuid, index.getOrElse(indexType))
  }

  @deprecated("Use delete(uuid: String, index: String) instead", "7.17.29")
  def delete(uuid: String, index: String, indexType: String): Boolean = {
    this.delete(uuid, index)
  }

  /** Delete an entity from the given index.
    * @param uuid
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   true if the entity was deleted successfully, false otherwise
    */
  def delete(uuid: String, index: String): Boolean

  /** Delete an entity from the given index asynchronously.
    * @param entity
    *   - the entity to delete
    * @param index
    *   - the name of the index to delete the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with true if the entity was deleted successfully, false otherwise
    */
  def deleteAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], ec: ExecutionContext): Future[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    deleteAsync(entity.uuid, index.getOrElse(indexType))
  }

  @deprecated("Use deleteAsync(uuid: String, index: String) instead", "7.17.29")
  def deleteAsync(uuid: String, index: String, indexType: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    this.deleteAsync(uuid, index)
  }

  /** Delete an entity from the given index asynchronously.
    * @param uuid
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   a Future that completes with true if the entity was deleted successfully, false otherwise
    */
  def deleteAsync(uuid: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    Future {
      this.delete(uuid, index)
    }
  }

}

trait BulkApi { _: RefreshApi with SettingsApi =>
  type A
  type R

  def toBulkAction(bulkItem: BulkItem): A

  implicit def toBulkElasticAction(a: A): BulkElasticAction

  implicit def toBulkElasticResult(r: R): BulkElasticResult

  def bulk(implicit bulkOptions: BulkOptions, system: ActorSystem): Flow[Seq[A], R, NotUsed]

  def bulkResult: Flow[R, Set[String], NotUsed]

  /** +----------+
    * |          |
    * |  Source  |  items: Iterator[D]
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * |transform | BulkableAction
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * | settings | Update elasticsearch settings (refresh and replicas)
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * |  group   |
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+        +----------+
    * |          |------->|          |
    * |  balance |        |   bulk   |
    * |          |------->|          |
    * +----------+        +----------+
    *                        |    |
    *                        |    |
    *                        |    |
    * +---------+            |    |
    * |         |<-----------'    |
    * |  merge  |                 |
    * |         |<----------------'
    * +---------+
    *      |
    *      v
    * +----------+
    * |          |
    * | result   | BulkResult
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * |   Sink   | indices: Set[String]
    * |          |
    * +----------+
    *
    * Asynchronously bulk items to Elasticsearch
    *
    * @param items         the items for which a bulk has to be performed
    * @param toDocument    the function to transform items to elastic documents in json format
    * @param idKey         the key mapping to the document id
    * @param suffixDateKey the key mapping to the date used to suffix the index
    * @param suffixDatePattern the date pattern used to suffix the index
    * @param update        whether to upsert or not the items
    * @param delete        whether to delete or not the items
    * @param parentIdKey   the key mapping to the elastic parent document id
    * @param bulkOptions   bulk options
    * @param system        actor system
    * @tparam D the type of the items
    * @return the indexes on which the documents have been indexed
    */
  def bulk[D](
    items: Iterator[D],
    toDocument: D => String,
    idKey: Option[String] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Set[String] = {

    implicit val materializer: Materializer = Materializer(system)

    import GraphDSL.Implicits._

    val source = Source.fromIterator(() => items)

    val sink = Sink.fold[Set[String], Set[String]](Set.empty[String])(_ ++ _)

    val g = Flow.fromGraph(GraphDSL.create() { implicit b =>
      val transform =
        b.add(
          Flow[D].map(item =>
            toBulkAction(
              toBulkItem(
                toDocument,
                idKey,
                suffixDateKey,
                suffixDatePattern,
                update,
                delete,
                parentIdKey,
                item
              )
            )
          )
        )

      val settings = b.add(BulkSettings[A](bulkOptions.disableRefresh)(this, toBulkElasticAction))

      val group = b.add(Flow[A].named("group").grouped(bulkOptions.maxBulkSize).map { items =>
//          logger.info(s"Preparing to write batch of ${items.size}...")
        items
      })

      val parallelism = Math.max(1, bulkOptions.balance)

      val bulkFlow: FlowShape[Seq[A], R] = b.add(bulk)

      val result = b.add(bulkResult)

      if (parallelism > 1) {
        val balancer = b.add(Balance[Seq[A]](parallelism))

        val merge = b.add(Merge[R](parallelism))

        transform ~> settings ~> group ~> balancer

        1 to parallelism foreach { _ =>
          balancer ~> bulkFlow ~> merge
        }

        merge ~> result
      } else {
        transform ~> settings ~> group ~> bulkFlow ~> result
      }

      FlowShape(transform.in, result.out)
    })

    val future = source.via(g).toMat(sink)(Keep.right).run()

    val indices = Await.result(future, Duration.Inf)
    indices.foreach(refresh)
    indices
  }

  def toBulkItem[D](
    toDocument: D => String,
    idKey: Option[String],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String],
    item: D
  )(implicit bulkOptions: BulkOptions): BulkItem = {

    implicit val formats: DefaultFormats = org.json4s.DefaultFormats
    val document = toDocument(item)
    val jsonMap = parse(document, useBigDecimalForDouble = false).extract[Map[String, Any]]
    // extract id
    val id = idKey.flatMap { i =>
      jsonMap.get(i).map(_.toString)
    }

    // extract final index name
    val index = suffixDateKey
      .flatMap { s =>
        // Expecting a date field YYYY-MM-dd ...
        jsonMap.get(s).map { d =>
          val strDate = d.toString.substring(0, 10)
          val date = LocalDate.parse(strDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          date.format(
            suffixDatePattern
              .map(DateTimeFormatter.ofPattern)
              .getOrElse(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          )
        }
      }
      .map(s => s"${bulkOptions.index}-$s")
      // use suffix if available otherwise only index
      .getOrElse(bulkOptions.index)

    // extract parent key
    val parent = parentIdKey.flatMap { i =>
      jsonMap.get(i).map(_.toString)
    }

    val action = delete match {
      case Some(d) if d => BulkAction.DELETE
      case _ =>
        update match {
          case Some(u) if u => BulkAction.UPDATE
          case _            => BulkAction.INDEX
        }
    }

    BulkItem(index, action, document, id, parent)
  }

}

trait CountApi {

  /** Count the number of documents matching the given JSON query asynchronously.
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  def countAsync(query: JSONQuery)(implicit ec: ExecutionContext): Future[Option[Double]] = {
    Future {
      this.count(query)
    }
  }

  /** Count the number of documents matching the given JSON query.
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  def count(query: JSONQuery): Option[Double]

}

trait AggregateApi[T <: AggregateResult] {

  /** Aggregate the results of the given SQL query.
    * @param sqlQuery
    *   - the query to aggregate the results for
    * @return
    *   a sequence of aggregated results
    */
  def aggregate(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[_root_.scala.collection.Seq[T]]
}

trait SingleValueAggregateApi extends AggregateApi[SingleValueAggregateResult]

trait GetApi {

  /** Get an entity by its id from the given index.
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   an Option containing the entity if it was found, None otherwise
    */
  def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U]

  /** Get an entity by its id from the given index asynchronously.
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with an Option containing the entity if it was found, None otherwise
    */
  def getAsync[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[Option[U]] = {
    Future {
      this.get[U](id, index, maybeType)
    }
  }
}

trait SearchApi {

  /** Search for entities matching the given JSON query.
    * @param jsonQuery
    *   - the query to search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of entities matching the query
    */
  def search[U](jsonQuery: JSONQuery)(implicit m: Manifest[U], formats: Formats): List[U]

  /** Search for entities matching the given SQL query.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of entities matching the query
    */
  def search[U](sqlQuery: SQLQuery)(implicit m: Manifest[U], formats: Formats): List[U] = {
    sqlQuery.search match {
      case Some(searchRequest) =>
        val indices = collection.immutable.Seq(searchRequest.sources: _*)
        search[U](JSONQuery(searchRequest.query, indices))
      case None =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  /** Search for entities matching the given SQL query asynchronously.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a Future that completes with a list of entities matching the query
    */
  def searchAsync[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]] = Future(
    this.search[U](sqlQuery)
  )

  /** Search for entities matching the given JSON query with inner hits.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of tuples containing the main entity and a list of inner hits
    */
  def searchWithInnerHits[U, I](sqlQuery: SQLQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    sqlQuery.search match {
      case Some(searchRequest) =>
        val indices = collection.immutable.Seq(searchRequest.sources: _*)
        val jsonQuery = JSONQuery(searchRequest.query, indices)
        searchWithInnerHits(jsonQuery, innerField)(m1, m2, formats)
      case None =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  /** Search for entities matching the given JSON query with inner hits.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of tuples containing the main entity and a list of inner hits
    */
  def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])]

  /** Perform a multi-search operation with the given SQL query.
    * @param sqlQuery
    *   - the SQL query to perform the multi-search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of entities matching the queries in the multi-search request
    */
  def multiSearch[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], formats: Formats): List[List[U]] = {
    sqlQuery.multiSearch match {
      case Some(multiSearchRequest) =>
        val jsonQueries: JSONQueries = JSONQueries(
          collection.immutable
            .Seq(multiSearchRequest.requests.map { searchRequest =>
              JSONQuery(searchRequest.query, collection.immutable.Seq(searchRequest.sources: _*))
            }: _*)
            .toList
        )
        multiSearch[U](jsonQueries)
      case None =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  /** Perform a multi-search operation with the given JSON queries.
    * @param jsonQueries
    *   - the JSON queries to perform the multi-search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of entities matching the queries in the multi-search request
    */
  def multiSearch[U](
    jsonQueries: JSONQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]]

  /** Perform a multi-search operation with the given SQL query and inner hits.
    * @param sqlQuery
    *   - the SQL query to perform the multi-search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of tuples containing the main entity and a list of inner hits
    */
  def multiSearchWithInnerHits[U, I](sqlQuery: SQLQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    sqlQuery.multiSearch match {
      case Some(multiSearchRequest) =>
        val jsonQueries: JSONQueries = JSONQueries(
          collection.immutable
            .Seq(multiSearchRequest.requests.map { searchRequest =>
              JSONQuery(searchRequest.query, collection.immutable.Seq(searchRequest.sources: _*))
            }: _*)
            .toList
        )
        multiSearchWithInnerHits[U, I](jsonQueries, innerField)
      case None =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  /** Perform a multi-search operation with the given JSON queries and inner hits.
    * @param jsonQueries
    *   - the JSON queries to perform the multi-search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of tuples containing the main entity and a list of inner hits
    */
  def multiSearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]]
}
