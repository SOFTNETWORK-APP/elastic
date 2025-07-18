package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.sql.SQLQuery
import com.fasterxml.jackson.core.JsonParseException
import com.sksamuel.elastic4s.requests.searches.queries.matches.MatchAllQuery
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import app.softnetwork.persistence._
import app.softnetwork.serialization._
import app.softnetwork.elastic.model._
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.person.model.Person
import com.typesafe.scalalogging.StrictLogging
import org.json4s.Formats
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.io.ByteArrayInputStream
import _root_.java.nio.file.{Files, Paths}
import _root_.java.util.concurrent.TimeUnit
import _root_.java.util.UUID

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticClientSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers
    with StrictLogging {

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val formats: Formats = commonFormats

  def pClient: ElasticProvider[Person] with ElasticClientApi
  def sClient: ElasticProvider[Sample] with ElasticClientApi
  def bClient: ElasticProvider[Binary] with ElasticClientApi

  import scala.language.implicitConversions

  implicit def toSQLQuery(sqlQuery: String): SQLQuery = SQLQuery(sqlQuery)

  override def beforeAll(): Unit = {
    super.beforeAll()
    pClient.createIndex("person")
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  "Creating an index and then delete it" should "work fine" in {
    pClient.createIndex("create_delete")
    blockUntilIndexExists("create_delete")
    "create_delete" should beCreated

    pClient.deleteIndex("create_delete")
    blockUntilIndexNotExists("create_delete")
    "create_delete" should not(beCreated())
  }

  "Adding an alias" should "work" in {
    pClient.addAlias("person", "person_alias")

    doesAliasExists("person_alias") shouldBe true
  }

  private def settings: Map[String, String] = {
    elasticClient.execute {
      getSettings("person")
    } complete () match {
      case Success(s) => s.result.settingsForIndex("person")
      case Failure(f) => throw f
    }
  }

  "Toggle refresh" should "work" in {
    pClient.toggleRefresh("person", enable = false)

    settings.getOrElse("index.refresh_interval", "") shouldBe "-1"

    pClient.toggleRefresh("person", enable = true)
    settings.getOrElse("index.refresh_interval", "") shouldBe "1s"
  }

  "Updating number of replicas" should "work" in {
    pClient.setReplicas("person", 3)
    settings.getOrElse("index.number_of_replicas", "") shouldBe "3"

    pClient.setReplicas("person", 0)
    settings.getOrElse("index.number_of_replicas", "") shouldBe "0"
  }

  val persons: List[String] = List(
    """ { "uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21 12:00:00"} """,
    """ { "uuid": "A14", "name": "Moe Szyslak",   "birthDate": "1967-11-21 12:00:00"} """,
    """ { "uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09 21:00:00"} """
  )

  private val personsWithUpsert =
    persons :+ """ { "uuid": "A16", "name": "Barney Gumble2", "birthDate": "1969-05-09 21:00:00"} """

  val children: List[String] = List(
    """ { "parentId": "A16", "name": "Steve Gumble", "birthDate": "1999-05-09 21:00:00"} """,
    """ { "parentId": "A16", "name": "Josh Gumble", "birthDate": "1999-05-09 21:00:00"} """
  )

  "Bulk index valid json without id key and suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person1", "person", 2)
    val indices = pClient.bulk[String](persons.iterator, identity, None, None, None)

    indices should contain only "person1"

    blockUntilCount(3, "person1")

    "person1" should haveCount(3)

    val response = elasticClient.execute {
      search("person1").query(MatchAllQuery())
    } complete ()

    response.result.hits.hits.foreach { h =>
      h.id should not be h.sourceField("uuid")
    }

    response.result.hits.hits
      .map(
        _.sourceField("name")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
  }

  "Bulk index valid json with an id key but no suffix key" should "work" in {
    elasticClient.execute(
      createIndex("person2").mapping(
        properties(
          objectField("child").copy(properties = Seq(textField("name").copy(index = Some(false))))
        )
      )
    ) complete () match {
      case Success(_) => ()
      case Failure(f) => throw f
    }

    implicit val bulkOptions: BulkOptions = BulkOptions("person2", "person", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    refresh(indices)

    indices should contain only "person2"

    blockUntilCount(3, "person2")

    "person2" should haveCount(3)

    val response = elasticClient.execute {
      search("person2").query(MatchAllQuery())
    } complete ()

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceField("uuid")
    }

    response.result.hits.hits
      .map(
        _.sourceField("name")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")

  }

  "Bulk index valid json with an id key and a suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person", "person", 1000)
    val indices =
      pClient.bulk[String](persons.iterator, identity, Some("uuid"), Some("birthDate"), None, None)
    refresh(indices)

    indices should contain allOf ("person-1967-11-21", "person-1969-05-09")

    blockUntilCount(2, "person-1967-11-21")
    blockUntilCount(1, "person-1969-05-09")

    "person-1967-11-21" should haveCount(2)
    "person-1969-05-09" should haveCount(1)

    val response = elasticClient.execute {
      search("person-1967-11-21", "person-1969-05-09").query(MatchAllQuery())
    } complete ()

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceField("uuid")
    }

    response.result.hits.hits
      .map(
        _.sourceField("name")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
  }

  "Bulk index invalid json with an id key and a suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person_error", "person", 1000)
    intercept[JsonParseException] {
      val invalidJson = persons :+ "fail"
      pClient.bulk[String](invalidJson.iterator, identity, None, None, None)
    }
  }

  "Bulk upsert valid json with an id key but no suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person4", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person4"

    blockUntilCount(3, "person4")

    "person4" should haveCount(3)

    val response = elasticClient.execute {
      search("person4").query(MatchAllQuery())
    } complete ()

    logger.info(s"response: ${response.result.hits.hits.mkString("\n")}")

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceAsMap.getOrElse("uuid", "")
    }

    response.result.hits.hits
      .map(
        _.sourceAsMap.getOrElse("name", "")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble2")
  }

  "Bulk upsert valid json with an id key and a suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person5", "person", 1000)
    val indices = pClient.bulk[String](
      personsWithUpsert.iterator,
      identity,
      Some("uuid"),
      Some("birthDate"),
      None,
      Some(true)
    )
    refresh(indices)

    indices should contain allOf ("person5-1967-11-21", "person5-1969-05-09")

    blockUntilCount(2, "person5-1967-11-21")
    blockUntilCount(1, "person5-1969-05-09")

    "person5-1967-11-21" should haveCount(2)
    "person5-1969-05-09" should haveCount(1)

    val response = elasticClient.execute {
      search("person5-1967-11-21", "person5-1969-05-09").query(MatchAllQuery())
    } complete ()

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceAsMap.getOrElse("uuid", "")
    }

    response.result.hits.hits
      .map(
        _.sourceAsMap.getOrElse("name", "")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble2")
  }

  "Count" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person6", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person6"

    blockUntilCount(3, "person6")

    "person6" should haveCount(3)

    import scala.collection.immutable.Seq

    pClient.countAsync(JSONQuery("{}", Seq[String]("person6"), Seq[String]())) complete () match {
      case Success(s) => s.getOrElse(0d).toInt should ===(3)
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Search" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person7", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person7"

    blockUntilCount(3, "person7")

    "person7" should haveCount(3)

    pClient.searchAsync[Person]("select * from person7") onComplete {
      case Success(r) =>
        r.size should ===(3)
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
      case Failure(f) => fail(f.getMessage)
    }

    pClient.searchAsync[Person](SQLQuery("select * from person7 where _id=\"A16\"")) onComplete {
      case Success(r) =>
        r.size should ===(1)
        r.map(_.uuid) should contain("A16")
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Get all" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person8", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person8"

    blockUntilCount(3, "person8")

    "person8" should haveCount(3)

    val response = pClient.search[Person]("select * from person8")

    response.size should ===(3)

  }

  "Get" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person9", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person9"

    blockUntilCount(3, "person9")

    "person9" should haveCount(3)

    val response = pClient.get[Person]("A16", Some("person9"))

    response.isDefined shouldBe true
    response.get.uuid shouldBe "A16"

  }

  "Index" should "work" in {
    val uuid = UUID.randomUUID().toString
    val sample = Sample(uuid)
    val result = sClient.index(sample)
    result shouldBe true

    val result2 = sClient.get[Sample](uuid)
    result2 match {
      case Some(r) =>
        r.uuid shouldBe uuid
      case _ =>
        fail("Sample not found")
    }
  }

  "Update" should "work" in {
    val uuid = UUID.randomUUID().toString
    val sample = Sample(uuid)
    val result = sClient.update(sample)
    result shouldBe true

    val result2 = sClient.get[Sample](uuid)
    result2 match {
      case Some(r) =>
        r.uuid shouldBe uuid
      case _ =>
        fail("Sample not found")
    }
  }

  "Delete" should "work" in {
    val uuid = UUID.randomUUID().toString
    val sample = Sample(uuid)
    val result = sClient.index(sample)
    result shouldBe true

    val result2 = sClient.delete(sample.uuid, Some("sample"))
    result2 shouldBe true

    val result3 = sClient.get[Sample](uuid)
    result3.isEmpty shouldBe true
  }

  "Index binary data" should "work" in {
    bClient.createIndex("binaries") shouldBe true
    val mapping =
      """{
        |    "properties": {
        |      "uuid": {
        |        "type": "keyword",
        |        "index": true
        |      },
        |      "createdDate": {
        |        "type": "date"
        |      },
        |      "lastUpdated": {
        |        "type": "date"
        |      },
        |      "content": {
        |        "type": "binary"
        |      },
        |      "md5": {
        |        "type": "keyword"
        |      }
        |    }
        |}
      """.stripMargin
    bClient.setMapping("binaries", mapping) shouldBe true
    for (uuid <- Seq("png", "jpg", "pdf")) {
      val path =
        Paths.get(Thread.currentThread().getContextClassLoader.getResource(s"avatar.$uuid").getPath)
      import app.softnetwork.utils.ImageTools._
      import app.softnetwork.utils.HashTools._
      import app.softnetwork.utils.Base64Tools._
      val encoded = encodeImageBase64(path).getOrElse("")
      val binary = Binary(
        uuid,
        content = encoded,
        md5 = hashStream(new ByteArrayInputStream(decodeBase64(encoded))).getOrElse("")
      )
      bClient.index(binary) shouldBe true
      bClient.get[Binary](uuid) match {
        case Some(result) =>
          val decoded = decodeBase64(result.content)
          val out = Paths.get(s"/tmp/${path.getFileName}")
          val fos = Files.newOutputStream(out)
          fos.write(decoded)
          fos.close()
          hashFile(out).getOrElse("") shouldBe binary.md5
        case _ => fail("no result found for \"" + uuid + "\"")
      }
    }
  }
}
