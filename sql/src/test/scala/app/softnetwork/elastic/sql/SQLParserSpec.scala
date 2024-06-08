package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object Queries {
  val numericalEq = "select $t.col1,$t.col2 from Table as $t where $t.identifier = 1.0"
  val numericalLt = "select * from Table where identifier < 1"
  val numericalLe = "select * from Table where identifier <= 1"
  val numericalGt = "select * from Table where identifier > 1"
  val numericalGe = "select * from Table where identifier >= 1"
  val numericalNe = "select * from Table where identifier <> 1"
  val literalEq = """select * from Table where identifier = "un""""
  val literalLt = "select * from Table where createdAt < \"now-35M/M\""
  val literalLe = "select * from Table where createdAt <= \"now-35M/M\""
  val literalGt = "select * from Table where createdAt > \"now-35M/M\""
  val literalGe = "select * from Table where createdAt >= \"now-35M/M\""
  val literalNe = """select * from Table where identifier <> "un""""
  val boolEq = """select * from Table where identifier = true"""
  val boolNe = """select * from Table where identifier <> false"""
  val literalLike = """select * from Table where identifier like "%un%""""
  val betweenExpression = """select * from Table where identifier between "1" and "2""""
  val andPredicate = "select * from Table where (identifier1 = 1) and (identifier2 > 2)"
  val orPredicate = "select * from Table where (identifier1 = 1) or (identifier2 > 2)"
  val leftPredicate =
    "select * from Table where ((identifier1 = 1) and (identifier2 > 2)) or (identifier3 = 3)"
  val rightPredicate =
    "select * from Table where (identifier1 = 1) and ((identifier2 > 2) or (identifier3 = 3))"
  val predicates =
    "select * from Table where ((identifier1 = 1) and (identifier2 > 2)) or ((identifier3 = 3) and (identifier4 = 4))"
  val nestedPredicate =
    "select * from Table where (identifier1 = 1) and nested((nested.identifier2 > 2) or (nested.identifier3 = 3))"
  val nestedCriteria =
    "select * from Table where (identifier1 = 1) and nested(nested.identifier3 = 3)"
  val childPredicate =
    "select * from Table where (identifier1 = 1) and child((child.identifier2 > 2) or (child.identifier3 = 3))"
  val childCriteria = "select * from Table where (identifier1 = 1) and child(child.identifier3 = 3)"
  val parentPredicate =
    "select * from Table where (identifier1 = 1) and parent((parent.identifier2 > 2) or (parent.identifier3 = 3))"
  val parentCriteria =
    "select * from Table where (identifier1 = 1) and parent(parent.identifier3 = 3)"
  val inLiteralExpression = "select * from Table where identifier in (\"val1\",\"val2\",\"val3\")"
  val inNumericalExpressionWithIntValues = "select * from Table where identifier in (1,2,3)"
  val inNumericalExpressionWithDoubleValues =
    "select * from Table where identifier in (1.0,2.1,3.4)"
  val notInLiteralExpression =
    "select * from Table where identifier not in (\"val1\",\"val2\",\"val3\")"
  val notInNumericalExpressionWithIntValues = "select * from Table where identifier not in (1,2,3)"
  val notInNumericalExpressionWithDoubleValues =
    "select * from Table where identifier not in (1.0,2.1,3.4)"
  val nestedWithBetween =
    "select * from Table where nested((ciblage.Archivage_CreationDate between \"now-3M/M\" and \"now\") and (ciblage.statutComportement = 1))"
  val count = "select count($t.id) as c1 from Table as t where $t.nom = \"Nom\""
  val countDistinct = "select count(distinct $t.id) as c2 from Table as t where $t.nom = \"Nom\""
  val countNested =
    "select count(email.value) as email from crmgp where profile.postalCode in (\"75001\",\"75002\")"
  val isNull = "select * from Table where identifier is null"
  val isNotNull = "select * from Table where identifier is not null"
  val geoDistanceCriteria =
    "select * from Table where distance(profile.location,(-70.0,40.0)) <= \"5km\""
}

/** Created by smanciot on 15/02/17.
  */
class SQLParserSpec extends AnyFlatSpec with Matchers {

  import Queries._

  "SQLParser" should "parse numerical eq" in {
    val result = SQLParser(numericalEq)
    result.right.get.sql should ===(numericalEq)
  }

  it should "parse numerical ne" in {
    val result = SQLParser(numericalNe)
    result.right.get.sql should ===(numericalNe)
  }

  it should "parse numerical lt" in {
    val result = SQLParser(numericalLt)
    result.right.get.sql should ===(numericalLt)
  }

  it should "parse numerical le" in {
    val result = SQLParser(numericalLe)
    result.right.get.sql should ===(numericalLe)
  }

  it should "parse numerical gt" in {
    val result = SQLParser(numericalGt)
    result.right.get.sql should ===(numericalGt)
  }

  it should "parse numerical ge" in {
    val result = SQLParser(numericalGe)
    result.right.get.sql should ===(numericalGe)
  }

  it should "parse literal eq" in {
    val result = SQLParser(literalEq)
    result.right.get.sql should ===(literalEq)
  }

  it should "parse literal like" in {
    val result = SQLParser(literalLike)
    result.right.get.sql should ===(literalLike)
  }

  it should "parse literal ne" in {
    val result = SQLParser(literalNe)
    result.right.get.sql should ===(literalNe)
  }

  it should "parse literal lt" in {
    val result = SQLParser(literalLt)
    result.right.get.sql should ===(literalLt)
  }

  it should "parse literal le" in {
    val result = SQLParser(literalLe)
    result.right.get.sql should ===(literalLe)
  }

  it should "parse literal gt" in {
    val result = SQLParser(literalGt)
    result.right.get.sql should ===(literalGt)
  }

  it should "parse literal ge" in {
    val result = SQLParser(literalGe)
    result.right.get.sql should ===(literalGe)
  }

  it should "parse boolean eq" in {
    val result = SQLParser(boolEq)
    result.right.get.sql should ===(boolEq)
  }

  it should "parse boolean ne" in {
    val result = SQLParser(boolNe)
    result.right.get.sql should ===(boolNe)
  }

  it should "parse between" in {
    val result = SQLParser(betweenExpression)
    result.right.get.sql should ===(betweenExpression)
  }

  it should "parse and predicate" in {
    val result = SQLParser(andPredicate)
    result.right.get.sql should ===(andPredicate)
  }

  it should "parse or predicate" in {
    val result = SQLParser(orPredicate)
    result.right.get.sql should ===(orPredicate)
  }

  it should "parse left predicate with criteria" in {
    val result = SQLParser(leftPredicate)
    result.right.get.sql should ===(leftPredicate)
  }

  it should "parse right predicate with criteria" in {
    val result = SQLParser(rightPredicate)
    result.right.get.sql should ===(rightPredicate)
  }

  it should "parse multiple predicates" in {
    val result = SQLParser(predicates)
    result.right.get.sql should ===(predicates)
  }

  it should "parse nested predicate" in {
    val result = SQLParser(nestedPredicate)
    result.right.get.sql should ===(nestedPredicate)
  }

  it should "parse nested criteria" in {
    val result = SQLParser(nestedCriteria)
    result.right.get.sql should ===(nestedCriteria)
  }

  it should "parse child predicate" in {
    val result = SQLParser(childPredicate)
    result.right.get.sql should ===(childPredicate)
  }

  it should "parse child criteria" in {
    val result = SQLParser(childCriteria)
    result.right.get.sql should ===(childCriteria)
  }

  it should "parse parent predicate" in {
    val result = SQLParser(parentPredicate)
    result.right.get.sql should ===(parentPredicate)
  }

  it should "parse parent criteria" in {
    val result = SQLParser(parentCriteria)
    result.right.get.sql should ===(parentCriteria)
  }

  it should "parse in literal expression" in {
    val result = SQLParser(inLiteralExpression)
    result.right.get.sql should ===(inLiteralExpression)
  }

  it should "parse in numerical expression with Int values" in {
    val result = SQLParser(inNumericalExpressionWithIntValues)
    result.right.get.sql should ===(inNumericalExpressionWithIntValues)
  }

  it should "parse in numerical expression with Double values" in {
    val result = SQLParser(inNumericalExpressionWithDoubleValues)
    result.right.get.sql should ===(inNumericalExpressionWithDoubleValues)
  }

  it should "parse not in literal expression" in {
    val result = SQLParser(notInLiteralExpression)
    result.right.get.sql should ===(notInLiteralExpression)
  }

  it should "parse not in numerical expression with Int values" in {
    val result = SQLParser(notInNumericalExpressionWithIntValues)
    result.right.get.sql should ===(notInNumericalExpressionWithIntValues)
  }

  it should "parse not in numerical expression with Double values" in {
    val result = SQLParser(notInNumericalExpressionWithDoubleValues)
    result.right.get.sql should ===(notInNumericalExpressionWithDoubleValues)
  }

  it should "parse nested with between" in {
    val result = SQLParser(nestedWithBetween)
    result.right.get.sql should ===(nestedWithBetween)
  }

  it should "parse count" in {
    val result = SQLParser(count)
    result.right.get.sql should ===(count)
  }

  it should "parse distinct count" in {
    val result = SQLParser(countDistinct)
    result.right.get.sql should ===(countDistinct)
  }

  it should "parse count with nested criteria" in {
    val result = SQLParser(countNested)
    result.right.get.sql should ===(
      "select count(email.value) as email from crmgp where nested(profile.postalCode in (\"75001\",\"75002\"))"
    )
  }

  it should "parse is null" in {
    val result = SQLParser(isNull)
    result.right.get.sql should ===(isNull)
  }

  it should "parse is not null" in {
    val result = SQLParser(isNotNull)
    result.right.get.sql should ===(isNotNull)
  }

  it should "parse geo distance criteria" in {
    val result = SQLParser(geoDistanceCriteria)
    result.right.get.sql should ===(geoDistanceCriteria)
  }

}
