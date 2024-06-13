package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Created by smanciot on 13/04/17.
  */
class ElasticQuerySpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  "ElasticQuery" should "perform native count" in {
    val results = ElasticQuery.aggregate(
      SQLQuery("select count(t.id) c2 from Table t where t.nom = \"Nom\"")
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe false
    result.aggName shouldBe "agg_id"
    result.field shouldBe "c2"
    result.sources shouldBe Seq[String]("Table")
    result.query shouldBe
    """|{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "agg_id": {
        |      "value_count": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform count distinct" in {
    val results = ElasticQuery.aggregate(
      SQLQuery("select count(distinct t.id) as c2 from Table as t where nom = \"Nom\"")
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe true
    result.aggName shouldBe "agg_distinct_id"
    result.field shouldBe "c2"
    result.sources shouldBe Seq[String]("Table")
    result.query shouldBe
    """|{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "agg_distinct_id": {
        |      "cardinality": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count" in {
    val results = ElasticQuery.aggregate(
      SQLQuery(
        "select count(inner_emails.value) as email from index i, unnest(emails) as inner_emails where i.nom = \"Nom\""
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_emails.agg_emails_value"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "agg_emails_value": {
        |          "value_count": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with nested criteria" in {
    val results = ElasticQuery.aggregate(
      SQLQuery(
        "select count(inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\"))"
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_emails.agg_emails_value"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool":{"filter":[{"bool": {
        |      "filter": [
        |          {
        |            "term": {
        |              "nom": {
        |                "value": "Nom"
        |              }
        |            }
        |          },
        |          {
        |            "nested": {
        |              "path": "profiles",
        |              "query": {
        |                "terms": {
        |                  "profiles.postalCode": [
        |                    "75001",
        |                    "75002"
        |                  ]
        |                }
        |              },
        |              "inner_hits":{"name":"inner_profiles"}
        |            }
        |          }
        |      ]
        |    }
        |  }]}},
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "agg_emails_value": {
        |          "value_count": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with filter" in {
    val results = ElasticQuery.aggregate(
      SQLQuery(
        "select count(inner_emails.value) as count_emails filter[inner_emails.context = \"profile\"] from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\"))"
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_emails.filtered_agg.agg_emails_value"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool":{"filter":[{"bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "profiles",
        |            "query": {
        |              "terms": {
        |                "profiles.postalCode": [
        |                  "75001",
        |                  "75002"
        |                ]
        |              }
        |            },
        |            "inner_hits":{"name":"inner_profiles"}
        |          }
        |        }
        |      ]
        |    }
        |  }]}},
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "filtered_agg": {
        |          "filter": {
        |            "term": {
        |              "emails.context": {
        |                "value": "profile"
        |              }
        |            }
        |          },
        |          "aggs": {
        |            "agg_emails_value": {
        |              "value_count": {
        |                "field": "emails.value"
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with \"and not\" operator" in {
    val results = ElasticQuery.aggregate(
      SQLQuery(
        "select count(distinct inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where ((inner_profiles.postalCode = \"33600\") and (inner_profiles.postalCode <> \"75001\"))"
      )
    )
    results.size shouldBe 1
    val result = results.head
    println(result.query)
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "nested_emails.agg_distinct_emails_value"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "nested": {
        |            "path": "profiles",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "term": {
        |                      "profiles.postalCode": {
        |                        "value": "33600"
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "bool": {
        |                      "must_not": [
        |                        {
        |                          "term": {
        |                            "profiles.postalCode": {
        |                              "value": "75001"
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "inner_profiles"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "agg_distinct_emails_value": {
        |          "cardinality": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}
        |""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with date filtering" in {
    val results = ElasticQuery.aggregate(
      SQLQuery(
        "select count(distinct inner_emails.value) as count_distinct_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where inner_profiles.postalCode = \"33600\" and inner_profiles.createdDate <= \"now-35M/M\""
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "nested_emails.agg_distinct_emails_value"
    result.field shouldBe "count_distinct_emails"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
    "query": {
      |        "bool": {
      |            "filter": [
      |                {
      |                    "nested": {
      |                        "path": "profiles",
      |                        "query": {
      |                            "bool": {
      |                                "filter": [
      |                                    {
      |                                        "term": {
      |                                            "profiles.postalCode": {
      |                                                "value": "33600"
      |                                            }
      |                                        }
      |                                    },
      |                                    {
      |                                        "range": {
      |                                            "profiles.createdDate": {
      |                                                "lte": "now-35M/M"
      |                                            }
      |                                        }
      |                                    }
      |                                ]
      |                            }
      |                        },
      |                        "inner_hits": {
      |                            "name": "inner_profiles"
      |                        }
      |                    }
      |                }
      |            ]
      |        }
      |    },
      |    "size": 0,
      |    "aggs": {
      |        "nested_emails": {
      |            "nested": {
      |                "path": "emails"
      |            },
      |            "aggs": {
      |                "agg_distinct_emails_value": {
      |                    "cardinality": {
      |                        "field": "emails.value"
      |                    }
      |                }
      |            }
      |        }
      |    }
      |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested select" in {
    val select = ElasticQuery.select(
      SQLQuery("""
        |SELECT
        |profileId,
        |profile_ccm.email as email,
        |profile_ccm.city as city,
        |profile_ccm.firstName as firstName,
        |profile_ccm.lastName as lastName,
        |profile_ccm.postalCode as postalCode,
        |profile_ccm.birthYear as birthYear
        |FROM index, unnest(profiles) as profile_ccm
        |WHERE
        |((profile_ccm.postalCode BETWEEN "10" AND "99999")
        |AND
        |(profile_ccm.birthYear <= 2000))
        |limit 100""".stripMargin)
    )
    select.isDefined shouldBe true
    val result = select.get
    result.query shouldBe
    """{
      |    "query": {
      |        "bool": {
      |            "filter": [
      |                {
      |                    "nested": {
      |                        "path": "profiles",
      |                        "query": {
      |                            "bool": {
      |                                "filter": [
      |                                    {
      |                                        "range": {
      |                                            "profiles.postalCode": {
      |                                                "gte": "10",
      |                                                "lte": "99999"
      |                                            }
      |                                        }
      |                                    },
      |                                    {
      |                                        "range": {
      |                                            "profiles.birthYear": {
      |                                                "lte": "2000"
      |                                            }
      |                                        }
      |                                    }
      |                                ]
      |                            }
      |                        },
      |                        "inner_hits": {
      |                            "name": "profile_ccm"
      |                        }
      |                    }
      |                }
      |            ]
      |        }
      |    },
      |    "from": 0,
      |    "size": 100,
      |    "_source": {
      |        "includes": [
      |            "profileId",
      |            "profile_ccm.email",
      |            "profile_ccm.city",
      |            "profile_ccm.firstName",
      |            "profile_ccm.lastName",
      |            "profile_ccm.postalCode",
      |            "profile_ccm.birthYear"
      |        ]
      |    }
      |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "handle complex query" in {
    val select = ElasticQuery.select(
      SQLQuery(
        s"""SELECT
           |  inner_products.name, inner_products.category, inner_products.price
           |FROM
           |  stores-dev store,
           |  UNNEST(store.products) as inner_products
           |WHERE
           |  firstName is not null AND
           |  lastName is not null AND
           |  description is not null AND
           |  store.deliveryPeriods.dayOfWeek=6 AND (
           |    distance(pickup.location,(0.0,0.0)) <= "7000m" OR
           |    distance(withdrawals.location,(0.0,0.0)) <= "7000m"
           |  ) AND
           |  preparationTime <= 120 AND
           |  NOT receiptOfOrdersDisabled=true AND
           |  blockedCustomers not like "uuid" AND (
           |    (inner_products.category in ("category1","category2")) AND
           |    (inner_products.price <= 100)
           |  )
           |LIMIT 100""".stripMargin
      )
    )
    select.isDefined shouldBe true
    val result = select.get
    println(result.query)
  }

  it should "just do it" in {
    val select = ElasticQuery.aggregate(
      SQLQuery(
        """
          |SELECT
          |  sum(distinct products.code) filter[products.category in ("categorie1", "categorie2")]
          |FROM store as store, unnest(store.products) as products
          |WHERE store.uuid = "uuid"""".stripMargin
      )
    )
    val result = select.head
    println(result.query)
  }
}
