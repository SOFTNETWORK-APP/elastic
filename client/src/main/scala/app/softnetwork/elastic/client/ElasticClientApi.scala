package app.softnetwork.elastic.client

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import akka.NotUsed
import akka.actor.ActorSystem
import _root_.akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl._
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization._
import app.softnetwork.elastic.sql.{SQLQueries, SQLQuery}
import com.typesafe.config.{Config, ConfigFactory}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag

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

  def createIndex(index: String, settings: String = defaultSettings): Boolean

  def deleteIndex(index: String): Boolean

  def closeIndex(index: String): Boolean

  def openIndex(index: String): Boolean
}

trait AliasApi {
  def addAlias(index: String, alias: String): Boolean
}

trait SettingsApi { _: IndicesApi =>
  def toggleRefresh(index: String, enable: Boolean): Unit = {
    updateSettings(
      index,
      if (!enable) """{"index" : {"refresh_interval" : -1} }"""
      else """{"index" : {"refresh_interval" : "1s"} }"""
    )
  }

  def setReplicas(index: String, replicas: Int): Unit = {
    updateSettings(index, s"""{"index" : {"number_of_replicas" : $replicas} }""")
  }

  def updateSettings(index: String, settings: String = defaultSettings): Boolean

  def loadSettings(): String
}

trait MappingApi {
  def setMapping(index: String, _type: String, mapping: String): Boolean
  def getMapping(index: String, _type: String): String
}

trait RefreshApi {
  def refresh(index: String): Boolean
}

trait FlushApi {
  def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean
}

trait IndexApi { _: RefreshApi =>
  def index[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    val _type = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this.index(
      index.getOrElse(_type),
      _type,
      entity.uuid,
      serialization.write[U](entity)
    )
  }

  def index(index: String, _type: String, id: String, source: String): Boolean

  def indexAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], ec: ExecutionContext, formats: Formats): Future[Boolean] = {
    val _type = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    indexAsync(index.getOrElse(_type), _type, entity.uuid, serialization.write[U](entity))
  }

  def indexAsync(index: String, _type: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean]
}

trait UpdateApi { _: RefreshApi =>
  def update[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    val _type = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this.update(
      index.getOrElse(_type),
      _type,
      entity.uuid,
      serialization.write[U](entity),
      upsert
    )
  }

  def update(index: String, _type: String, id: String, source: String, upsert: Boolean): Boolean

  def updateAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], ec: ExecutionContext, formats: Formats): Future[Boolean] = {
    val _type = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this
      .updateAsync(
        index.getOrElse(_type),
        _type,
        entity.uuid,
        serialization.write[U](entity),
        upsert
      )
  }

  def updateAsync(index: String, _type: String, id: String, source: String, upsert: Boolean)(
    implicit ec: ExecutionContext
  ): Future[Boolean]
}

trait DeleteApi { _: RefreshApi =>
  def delete[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U]): Boolean = {
    val _type = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    delete(entity.uuid, index.getOrElse(_type), _type)
  }

  def delete(uuid: String, index: String, _type: String): Boolean

  def deleteAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], ec: ExecutionContext): Future[Boolean] = {
    val _type = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    deleteAsync(entity.uuid, index.getOrElse(_type), _type)
  }

  def deleteAsync(uuid: String, index: String, _type: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean]

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
    * |    |
    * |    |
    * |    |
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

    val body = action match {
      case BulkAction.UPDATE => docAsUpsert(document)
      case _                 => document
    }

    BulkItem(index, action, body, id, parent)
  }

}

trait CountApi {
  def countAsync(query: JSONQuery)(implicit ec: ExecutionContext): Future[Option[Double]]

  def count(query: JSONQuery): Option[Double]

}

trait AggregateApi[T <: AggregateResult] {
  def aggregate(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[_root_.scala.collection.Seq[T]]
}

trait SingleValueAggregateApi extends AggregateApi[SingleValueAggregateResult]

trait GetApi {
  def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U]

  def getAsync[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[Option[U]]
}

trait SearchApi {

  def search[U](jsonQuery: JSONQuery)(implicit m: Manifest[U], formats: Formats): List[U]

  def search[U](sqlQuery: SQLQuery)(implicit m: Manifest[U], formats: Formats): List[U]

  def searchAsync[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]]

  def searchWithInnerHits[U, I](sqlQuery: SQLQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])]

  def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])]

  def multiSearch[U](
    sqlQueries: SQLQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]]

  def multiSearch[U](
    jsonQueries: JSONQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]]

  def multiSearchWithInnerHits[U, I](sqlQueries: SQLQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]]

  def multiSearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]]

}
