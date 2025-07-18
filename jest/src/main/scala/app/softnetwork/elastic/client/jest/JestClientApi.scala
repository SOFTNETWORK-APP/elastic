package app.softnetwork.elastic.client.jest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql
import app.softnetwork.elastic.sql.{ElasticSearchRequest, SQLQuery}
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization._
import io.searchbox.action.BulkableAction
import io.searchbox.core._
import io.searchbox.core.search.aggregation.RootAggregation
import io.searchbox.indices._
import io.searchbox.indices.aliases.{AddAliasMapping, ModifyAliases, RemoveAliasMapping}
import io.searchbox.indices.mapping.{GetMapping, PutMapping}
import io.searchbox.indices.settings.{GetSettings, UpdateSettings}
import io.searchbox.params.Parameters
import org.json4s.Formats

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 20/05/2021.
  */
trait JestClientApi
    extends ElasticClientApi
    with JestIndicesApi
    with JestAliasApi
    with JestSettingsApi
    with JestMappingApi
    with JestRefreshApi
    with JestFlushApi
    with JestCountApi
    with JestSingleValueAggregateApi
    with JestIndexApi
    with JestUpdateApi
    with JestDeleteApi
    with JestGetApi
    with JestSearchApi
    with JestBulkApi

trait JestIndicesApi extends IndicesApi with JestClientCompanion {
  override def createIndex(index: String, settings: String = defaultSettings): Boolean =
    apply().execute(new CreateIndex.Builder(index).settings(settings).build()).isSucceeded

  override def deleteIndex(index: String): Boolean =
    apply().execute(new DeleteIndex.Builder(index).build()).isSucceeded

  override def closeIndex(index: String): Boolean =
    apply().execute(new CloseIndex.Builder(index).build()).isSucceeded

  override def openIndex(index: String): Boolean =
    apply().execute(new OpenIndex.Builder(index).build()).isSucceeded
}

trait JestAliasApi extends AliasApi with JestClientCompanion {
  override def addAlias(index: String, alias: String): Boolean = {
    apply()
      .execute(
        new ModifyAliases.Builder(
          new AddAliasMapping.Builder(index, alias).build()
        ).build()
      )
      .isSucceeded
  }

  override def removeAlias(index: String, alias: String): Boolean = {
    apply()
      .execute(
        new ModifyAliases.Builder(
          new RemoveAliasMapping.Builder(index, alias).build()
        ).build()
      )
      .isSucceeded
  }
}

trait JestSettingsApi extends SettingsApi with JestClientCompanion {
  _: IndicesApi =>
  override def updateSettings(index: String, settings: String = defaultSettings): Boolean =
    closeIndex(index) &&
    apply().execute(new UpdateSettings.Builder(settings).addIndex(index).build()).isSucceeded &&
    openIndex(index)

  override def loadSettings(): String =
    apply().execute(new GetSettings.Builder().build()).getJsonString
}

trait JestMappingApi extends MappingApi with JestClientCompanion {
  _: IndicesApi =>
  override def setMapping(index: String, indexType: String, mapping: String): Boolean =
    apply().execute(new PutMapping.Builder(index, indexType, mapping).build()).isSucceeded

  override def getMapping(index: String, indexType: String): String =
    apply()
      .execute(new GetMapping.Builder().addIndex(index).addType(indexType).build())
      .getJsonString
}

trait JestRefreshApi extends RefreshApi with JestClientCompanion {
  override def refresh(index: String): Boolean =
    apply().execute(new Refresh.Builder().addIndex(index).build()).isSucceeded
}

trait JestFlushApi extends FlushApi with JestClientCompanion {
  override def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean = apply()
    .execute(
      new Flush.Builder().addIndex(index).force(force).waitIfOngoing(wait).build()
    )
    .isSucceeded
}

trait JestCountApi extends CountApi with JestClientCompanion {
  override def count(jsonQuery: JSONQuery): Option[Double] = {
    import jsonQuery._
    val count = new Count.Builder().query(query)
    for (indice <- indices) count.addIndex(indice)
    for (t      <- types) count.addType(t)
    val result = apply().execute(count.build())
    if (!result.isSucceeded)
      logger.error(result.getErrorMessage)
    Option(result.getCount)
  }
}

trait JestSingleValueAggregateApi extends SingleValueAggregateApi with JestCountApi {
  override def aggregate(
    sqlQuery: SQLQuery
  )(implicit ec: ExecutionContext): Future[Seq[SingleValueAggregateResult]] = {
    val futures = for (aggregation <- sqlQuery.aggregations) yield {
      val promise: Promise[SingleValueAggregateResult] = Promise()
      val field = aggregation.field
      val sourceField = aggregation.sourceField
      val aggType = aggregation.aggType
      val aggName = aggregation.aggName
      val query = aggregation.query
      val sources = aggregation.sources
      sourceField match {
        case "_id" if aggType.sql == "count" =>
          countAsync(
            JSONQuery(
              query,
              collection.immutable.Seq(sources: _*),
              collection.immutable.Seq.empty[String]
            )
          ).onComplete {
            case Success(result) =>
              promise.success(
                SingleValueAggregateResult(
                  field,
                  aggType,
                  result.getOrElse(0d),
                  None
                )
              )
            case Failure(f) =>
              logger.error(f.getMessage, f.fillInStackTrace())
              promise.success(SingleValueAggregateResult(field, aggType, 0d, Some(f.getMessage)))
          }
          promise.future
        case _ =>
          import JestClientApi._
          import JestClientResultHandler._
          apply()
            .executeAsyncPromise(
              JSONQuery(
                query,
                collection.immutable.Seq(sources: _*),
                collection.immutable.Seq.empty[String]
              ).search
            )
            .onComplete {
              case Success(result) =>
                val agg = aggName.split("\\.").last

                val itAgg = aggName.split("\\.").iterator

                var root =
                  if (aggregation.nested)
                    result.getAggregations.getAggregation(itAgg.next(), classOf[RootAggregation])
                  else
                    result.getAggregations

                if (aggregation.filtered) {
                  root = root.getAggregation(itAgg.next(), classOf[RootAggregation])
                }

                promise.success(
                  SingleValueAggregateResult(
                    field,
                    aggType,
                    aggType match {
                      case sql.Count =>
                        if (aggregation.distinct)
                          root.getCardinalityAggregation(agg).getCardinality.doubleValue()
                        else {
                          root.getValueCountAggregation(agg).getValueCount.doubleValue()
                        }
                      case sql.Sum =>
                        root.getSumAggregation(agg).getSum
                      case sql.Avg =>
                        root.getAvgAggregation(agg).getAvg
                      case sql.Min =>
                        root.getMinAggregation(agg).getMin
                      case sql.Max =>
                        root.getMaxAggregation(agg).getMax
                      case _ => 0d
                    },
                    None
                  )
                )

              case Failure(f) =>
                logger.error(f.getMessage, f.fillInStackTrace())
                promise.success(SingleValueAggregateResult(field, aggType, 0d, Some(f.getMessage)))
            }

          promise.future
      }
    }
    Future.sequence(futures)
  }
}

trait JestIndexApi extends IndexApi with JestClientCompanion {
  _: RefreshApi =>
  override def index(index: String, indexType: String, id: String, source: String): Boolean = {
    Try(
      apply().execute(
        new Index.Builder(source).index(index).`type`(indexType).id(id).build()
      )
    ) match {
      case Success(s) =>
        if (!s.isSucceeded)
          logger.error(s.getErrorMessage)
        s.isSucceeded && this.refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

}

trait JestUpdateApi extends UpdateApi with JestClientCompanion {
  _: RefreshApi =>
  override def update(
    index: String,
    indexType: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    Try(
      apply().execute(
        new Update.Builder(
          if (upsert)
            docAsUpsert(source)
          else
            source
        ).index(index).`type`(indexType).id(id).build()
      )
    ) match {
      case Success(s) =>
        if (!s.isSucceeded)
          logger.error(s.getErrorMessage)
        s.isSucceeded && this.refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

}

trait JestDeleteApi extends DeleteApi with JestClientCompanion {
  _: RefreshApi =>
  override def delete(uuid: String, index: String, indexType: String): Boolean = {
    val result = apply().execute(
      new Delete.Builder(uuid).index(index).`type`(indexType).build()
    )
    if (!result.isSucceeded) {
      logger.error(result.getErrorMessage)
    }
    result.isSucceeded && this.refresh(index)
  }

}

trait JestGetApi extends GetApi with JestClientCompanion {

  // GetApi
  override def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U] = {
    val result = apply().execute(
      new Get.Builder(
        index.getOrElse(
          maybeType.getOrElse(
            m.runtimeClass.getSimpleName.toLowerCase
          )
        ),
        id
      ).build()
    )
    if (result.isSucceeded) {
      Some(serialization.read[U](result.getSourceAsString))
    } else {
      logger.error(result.getErrorMessage)
      None
    }
  }

}

trait JestSearchApi extends SearchApi with JestClientCompanion {

  import JestClientApi._

  override def search[U](
    jsonQuery: JSONQuery
  )(implicit m: Manifest[U], formats: Formats): List[U] = {
    import jsonQuery._
    val search = new Search.Builder(query)
    for (indice <- indices) search.addIndex(indice)
    for (t      <- types) search.addType(t)
    Try(
      apply()
        .execute(search.build())
        .getSourceAsStringList
        .asScala
        .map(source => serialization.read[U](source))
        .toList
    ) match {
      case Success(s) => s
      case Failure(f) =>
        logger.error(f.getMessage, f)
        List.empty
    }
  }

  override def search[U](sqlQuery: SQLQuery)(implicit m: Manifest[U], formats: Formats): List[U] = {
    val search: Option[Search] = sqlQuery.jestSearch
    (search match {
      case Some(s) =>
        val result = apply().execute(s)
        if (result.isSucceeded) {
          Some(result)
        } else {
          logger.error(result.getErrorMessage)
          None
        }
      case _ => None
    }) match {
      case Some(searchResult) =>
        Try(
          searchResult.getSourceAsStringList.asScala
            .map(source => serialization.read[U](source))
            .toList
        ) match {
          case Success(s) => s
          case Failure(f) =>
            logger.error(f.getMessage, f)
            List.empty
        }
      case _ => List.empty
    }
  }

  override def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    val result = apply().execute(jsonQuery.search)
    (if (result.isSucceeded) {
       Some(result)
     } else {
       logger.error(result.getErrorMessage)
       None
     }) match {
      case Some(searchResult) =>
        Try(searchResult.getJsonObject ~> [U, I] innerField) match {
          case Success(s) => s
          case Failure(f) =>
            logger.error(f.getMessage, f)
            List.empty
        }
      case _ => List.empty
    }
  }

  override def multiSearch[U](
    jsonQueries: JSONQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]] = {
    val searches: List[Search] = jsonQueries.queries.map(_.search)
    val multiSearchResult = apply().execute(new MultiSearch.Builder(searches.asJava).build())
    multiSearchResult.getResponses.asScala
      .map(searchResponse =>
        searchResponse.searchResult.getSourceAsStringList.asScala
          .map(source => serialization.read[U](source))
          .toList
      )
      .toList
  }

  override def multiSearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    val multiSearch = new MultiSearch.Builder(jsonQueries.queries.map(_.search).asJava).build()
    val multiSearchResult = apply().execute(multiSearch)
    if (multiSearchResult.isSucceeded) {
      multiSearchResult.getResponses.asScala
        .map(searchResponse => searchResponse.searchResult.getJsonObject ~> [U, I] innerField)
        .toList
    } else {
      logger.error(multiSearchResult.getErrorMessage)
      List.empty
    }
  }

}

trait JestBulkApi
    extends JestRefreshApi
    with JestSettingsApi
    with JestIndicesApi
    with BulkApi
    with JestClientCompanion {
  override type A = BulkableAction[DocumentResult]
  override type R = BulkResult

  override implicit def toBulkElasticAction(a: A): BulkElasticAction =
    new BulkElasticAction {
      override def index: String = a.getIndex
    }

  private[this] def toBulkElasticResultItem(i: BulkResult#BulkResultItem): BulkElasticResultItem =
    new BulkElasticResultItem {
      override def index: String = i.index
    }

  override implicit def toBulkElasticResult(r: R): BulkElasticResult =
    new BulkElasticResult {
      override def items: List[BulkElasticResultItem] =
        r.getItems.asScala.toList.map(toBulkElasticResultItem)
    }

  override def bulk(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[A], R, NotUsed] = {
    import JestClientResultHandler._
    val parallelism = Math.max(1, bulkOptions.balance)

    Flow[Seq[BulkableAction[DocumentResult]]]
      .named("bulk")
      .mapAsyncUnordered[BulkResult](parallelism)(items => {
        logger.info(s"Starting to write batch of ${items.size}...")
        val init =
          new Bulk.Builder().defaultIndex(bulkOptions.index).defaultType(bulkOptions.documentType)
        val bulkQuery = items.foldLeft(init) { (current, query) =>
          current.addAction(query)
        }
        apply().executeAsyncPromise(bulkQuery.build())
      })
  }

  override def bulkResult: Flow[R, Set[String], NotUsed] =
    Flow[BulkResult]
      .named("result")
      .map(result => {
        val items = result.getItems
        val indices = items.asScala.map(_.index).toSet
        logger.info(s"Finished to write batch of ${items.size} within ${indices.mkString(",")}.")
        indices
      })

  override def toBulkAction(bulkItem: BulkItem): A = {
    val builder = bulkItem.action match {
      case BulkAction.DELETE => new Delete.Builder(bulkItem.body)
      case BulkAction.UPDATE => new Update.Builder(docAsUpsert(bulkItem.body))
      case _                 => new Index.Builder(bulkItem.body)
    }
    bulkItem.id.foreach(builder.id)
    builder.index(bulkItem.index)
    bulkItem.parent.foreach(s => builder.setParameter(Parameters.PARENT, s))
    builder.build()
  }

}

object JestClientApi {

  implicit def requestToSearch(elasticSelect: ElasticSearchRequest): Search = {
    import elasticSelect._
    Console.println(query)
    val search = new Search.Builder(query)
    for (source <- sources) search.addIndex(source)
    search.build()
  }

  implicit class SearchSQLQuery(sqlQuery: SQLQuery) {
    def jestSearch: Option[Search] = {
      sqlQuery.search match {
        case Some(value) => Some(value)
        case None        => None
      }
    }
  }

  implicit class MultiSearchSQLQuery(sqlQuery: SQLQuery) {
    def jestMultiSearch: Option[MultiSearch] = {
      sqlQuery.multiSearch.map(m => {
        import m._

        import scala.collection.JavaConverters._
        Console.println(query)
        new MultiSearch.Builder(m.requests.map(requestToSearch).asJava).build()
      })
    }
  }

  implicit class SearchJSONQuery(jsonQuery: JSONQuery) {
    def search: Search = {
      import jsonQuery._
      val _search = new Search.Builder(query)
      for (indice <- indices) _search.addIndex(indice)
      for (t      <- types) _search.addType(t)
      _search.build()
    }
  }

  implicit class SearchResults(searchResult: SearchResult) {
    def apply[M: Manifest]()(implicit formats: Formats): List[M] = {
      searchResult.getSourceAsStringList.asScala.map(source => serialization.read[M](source)).toList
    }
  }

  implicit class JestBulkAction(bulkableAction: BulkableAction[DocumentResult]) {
    def index: String = bulkableAction.getIndex
  }
}
