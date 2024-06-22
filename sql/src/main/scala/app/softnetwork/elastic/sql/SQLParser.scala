package app.softnetwork.elastic.sql

import scala.util.parsing.combinator.RegexParsers

/** Created by smanciot on 27/06/2018.
  *
  * SQL Parser for ElasticSearch
  *
  * TODO implements SQL :
  *   - UNION,
  *   - JOIN,
  *   - GROUP BY,
  *   - ORDER BY,
  *   - HAVING, etc.
  */
object SQLParser extends RegexParsers {

  val regexAlias =
    """\b(?!(?i)except\b)\b(?!(?i)where\b)\b(?!(?i)filter\b)\b(?!(?i)from\b)[a-zA-Z0-9_]*"""

  def identifier: Parser[SQLIdentifier] =
    "(?i)distinct".r.? ~ """[\*a-zA-Z_\-][a-zA-Z0-9_\-\.\[\]\*]*""".r ^^ { case d ~ str =>
      SQLIdentifier(
        str,
        None,
        d
      )
    }

  def literal: Parser[SQLLiteral] =
    """"[^"]*"|'[^']*'""".r ^^ (str => SQLLiteral(str.substring(1, str.length - 1)))

  def int: Parser[SQLInt] = """(-)?(0|[1-9]\d*)""".r ^^ (str => SQLInt(str.toInt))

  def double: Parser[SQLDouble] = """(-)?(\d+\.\d+)""".r ^^ (str => SQLDouble(str.toDouble))

  def boolean: Parser[SQLBoolean] = """(true|false)""".r ^^ (bool => SQLBoolean(bool.toBoolean))

  def eq: Parser[SQLExpressionOperator] = "=" ^^ (_ => EQ)
  def ge: Parser[SQLExpressionOperator] = ">=" ^^ (_ => GE)
  def gt: Parser[SQLExpressionOperator] = ">" ^^ (_ => GT)
  def in: Parser[SQLExpressionOperator] = "(?i)in".r ^^ (_ => IN)
  def le: Parser[SQLExpressionOperator] = "<=" ^^ (_ => LE)
  def like: Parser[SQLExpressionOperator] = "(?i)like".r ^^ (_ => LIKE)
  def lt: Parser[SQLExpressionOperator] = "<" ^^ (_ => LT)
  def ne: Parser[SQLExpressionOperator] = "<>" ^^ (_ => NE)

  def isNull: Parser[SQLExpressionOperator] = "(?i)(is null)".r ^^ (_ => IS_NULL)
  def isNullExpression: Parser[SQLCriteria] = identifier ~ isNull ^^ { case i ~ _ => SQLIsNull(i) }

  def isNotNull: Parser[SQLExpressionOperator] = "(?i)(is not null)".r ^^ (_ => IS_NOT_NULL)
  def isNotNullExpression: Parser[SQLCriteria] = identifier ~ isNotNull ^^ { case i ~ _ =>
    SQLIsNotNull(i)
  }

  def equalityExpression: Parser[SQLExpression] =
    not.? ~ identifier ~ (eq | ne) ~ (boolean | literal | double | int) ^^ { case n ~ i ~ o ~ v =>
      SQLExpression(i, o, v, n)
    }
  def likeExpression: Parser[SQLExpression] = identifier ~ not.? ~ like ~ literal ^^ {
    case i ~ n ~ o ~ v =>
      SQLExpression(i, o, v, n)
  }
  def comparisonExpression: Parser[SQLExpression] =
    not.? ~ identifier ~ (ge | gt | le | lt) ~ (double | int | literal) ^^ { case n ~ i ~ o ~ v =>
      SQLExpression(i, o, v, n)
    }

  def inLiteralExpression: Parser[SQLCriteria] =
    identifier ~ not.? ~ in ~ start ~ rep1(literal ~ separator.?) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ => SQLIn(i, SQLLiteralValues(v map { _._1 }), n)
    }
  def inNumericalExpression: Parser[SQLCriteria] =
    identifier ~ not.? ~ in ~ start ~ rep1((double | int) ~ separator.?) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ => SQLIn(i, SQLNumericValues(v map { _._1 }), n)
    }

  def between: Parser[SQLExpressionOperator] = "(?i)between".r ^^ (_ => BETWEEN)
  def betweenExpression: Parser[SQLCriteria] = identifier ~ between ~ literal ~ and ~ literal ^^ {
    case i ~ _ ~ from ~ _ ~ to => SQLBetween(i, from, to)
  }

  def distance: Parser[SQLFunction] = "(?i)distance".r ^^ (_ => SQLDistance)
  def distanceExpression: Parser[SQLCriteria] =
    distance ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def matchExpression: Parser[ElasticMatch] =
    "(?i)match".r ~ start ~ identifier ~ separator ~ literal ~ separator.? ~ literal.? ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ l ~ _ ~ o ~ _ => ElasticMatch(i, l, o.map(_.value))
    }

  def start: Parser[SQLDelimiter] = "(" ^^ (_ => StartPredicate)
  def end: Parser[SQLDelimiter] = ")" ^^ (_ => EndPredicate)
  def separator: Parser[SQLDelimiter] = "," ^^ (_ => Separator)

  def and: Parser[SQLPredicateOperator] = "(?i)and".r ^^ (_ => AND)
  def or: Parser[SQLPredicateOperator] = "(?i)or".r ^^ (_ => OR)
  def not: Parser[NOT.type] = "(?i)not".r ^^ (_ => NOT)

  def nested: Parser[ElasticOperator] = "(?i)nested".r ^^ (_ => NESTED)
  def child: Parser[ElasticOperator] = "(?i)child".r ^^ (_ => CHILD)
  def parent: Parser[ElasticOperator] = "(?i)parent".r ^^ (_ => PARENT)

  def criteria: Parser[SQLCriteria] =
    (equalityExpression | likeExpression | comparisonExpression | inLiteralExpression | inNumericalExpression | betweenExpression | isNotNullExpression | isNullExpression | distanceExpression | matchExpression) ^^ (
      c => c
    )

  def predicate: Parser[SQLPredicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => SQLPredicate(l, o, r, n)
  }

  def nestedCriteria: Parser[ElasticRelation] = nested ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticNested(c)
  }
  def nestedPredicate: Parser[ElasticRelation] = nested ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticNested(p)
  }

  def childCriteria: Parser[ElasticRelation] = child ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticChild(c)
  }
  def childPredicate: Parser[ElasticRelation] = child ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticChild(p)
  }

  def parentCriteria: Parser[ElasticRelation] = parent ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticParent(c)
  }
  def parentPredicate: Parser[ElasticRelation] = parent ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticParent(p)
  }

  def alias: Parser[SQLAlias] = "(?i)as".r.? ~ regexAlias.r ^^ { case _ ~ b => SQLAlias(b) }

  def count: Parser[AggregateFunction] = "(?i)count".r ^^ (_ => Count)
  def min: Parser[AggregateFunction] = "(?i)min".r ^^ (_ => Min)
  def max: Parser[AggregateFunction] = "(?i)max".r ^^ (_ => Max)
  def avg: Parser[AggregateFunction] = "(?i)avg".r ^^ (_ => Avg)
  def sum: Parser[AggregateFunction] = "(?i)sum".r ^^ (_ => Sum)

  def _select: Parser[SELECT.type] = "(?i)select".r ^^ (_ => SELECT)

  def _filter: Parser[FILTER.type] = "(?i)filter".r ^^ (_ => FILTER)

  def _from: Parser[FROM.type] = "(?i)from".r ^^ (_ => FROM)

  def _where: Parser[WHERE.type] = "(?i)where".r ^^ (_ => WHERE)

  def _limit: Parser[LIMIT.type] = "(?i)limit".r ^^ (_ => LIMIT)

  def filter: Parser[SQLFilter] = _filter ~> "[" ~> whereCriteria <~ "]" ^^ { case rawTokens =>
    SQLFilter(
      processTokens(rawTokens) match {
        case Some(c) => Some(c)
        case _       => None
      }
    )
  }

  def field: Parser[SQLField] = identifier ~ alias.? ^^ { case i ~ a =>
    SQLField(i, a)
  }

  def except: Parser[SQLExcept] = "(?i)except".r ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      SQLExcept(e)
  }

  def aggregate: Parser[SQLAggregate] =
    (min | max | avg | sum | count) ~ start ~ identifier ~ end ~ alias.? ~ filter.? ^^ {
      case agg ~ _ ~ i ~ _ ~ a ~ f => new SQLAggregate(agg, i, a, f)
    }

  def selectAggregates: Parser[SQLSelect] = _select ~ rep1sep(aggregate, separator) ^^ {
    case _ ~ aggregates =>
      new SQLSelectAggregates(aggregates)
  }

  def select: Parser[SQLSelect] = _select ~ rep1sep(field, separator) ~ except.? ^^ {
    case _ ~ fields ~ e =>
      SQLSelect(fields, e)
  }

  def unnest: Parser[SQLTable] = "(?i)unnest".r ~ start ~ identifier ~ end ~ alias ^^ {
    case _ ~ _ ~ i ~ _ ~ a =>
      SQLTable(SQLUnnest(i), Some(a))
  }

  def table: Parser[SQLTable] = identifier ~ alias.? ^^ { case i ~ a => SQLTable(i, a) }

  def from: Parser[SQLFrom] = _from ~ rep1sep(unnest | table, separator) ^^ { case _ ~ tables =>
    SQLFrom(tables)
  }

  def allPredicate: SQLParser.Parser[SQLCriteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  def allCriteria: SQLParser.Parser[SQLCriteria] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: SQLParser.Parser[List[SQLToken]] = rep1(
    allPredicate | allCriteria | start | or | and | end
  )

  def where: Parser[SQLWhere] = _where ~ whereCriteria ^^ { case _ ~ rawTokens =>
    SQLWhere(processTokens(rawTokens))
  }

  def limit: SQLParser.Parser[SQLLimit] = _limit ~ int ^^ { case _ ~ i => SQLLimit(i.value) }

  def tokens: Parser[_ <: SQLSelectQuery] = {
    phrase((selectAggregates | select) ~ from ~ where.? ~ limit.?) ^^ { case s ~ f ~ w ~ l =>
      s match {
        case x: SQLSelectAggregates => new SQLSelectAggregatesQuery(x, f, w, l)
        case _                      => SQLSelectQuery(s, f, w, l)
      }
    }
  }

  def apply(query: String): Either[SQLParserError, SQLSelectQuery] = {
    parse(tokens, query) match {
      case NoSuccess(msg, _) =>
        println(msg)
        Left(SQLParserError(msg))
      case Success(result, _) => Right(result.update())
    }
  }

  import scala.annotation.tailrec

  /** This method is used to recursively process a list of SQL tokens and construct SQL criteria and
    * predicates from these tokens. Here are the key points:
    *
    * Base case (Nil): If the list of tokens is empty (Nil), we check the contents of the stack to
    * determine the final result.
    *
    * If the stack contains an operator, a left criterion and a right criterion, we create a
    * SQLPredicate predicate. Otherwise, we return the first criterion (SQLCriteria) of the stack if
    * it exists. Case of criteria (SQLCriteria): If the first token is a criterion, we treat it
    * according to the content of the stack:
    *
    * If the stack contains a predicate operator, we create a predicate with the left and right
    * criteria and update the stack. Otherwise, we simply add the criterion to the stack. Case of
    * operators (SQLPredicateOperator): If the first token is a predicate operator, we treat it
    * according to the contents of the stack:
    *
    * If the stack contains at least two elements, we create a predicate with the left and right
    * criterion and update the stack. If the stack contains only one element (a single operator), we
    * simply add the operator to the stack. Otherwise, it's a battery status error. Case of
    * delimiters (StartDelimiter and EndDelimiter): If the first token is a start delimiter
    * (StartDelimiter), we extract the tokens up to the corresponding end delimiter (EndDelimiter),
    * we recursively process the extracted sub-tokens, then we continue with the rest of the tokens.
    *
    * Other cases: If none of the previous cases match, an IllegalStateException is thrown to
    * indicate an unexpected token type.
    *
    * @param tokens
    *   - liste des tokens SQL
    * @param stack
    *   - stack de tokens
    * @return
    */
  @tailrec
  private def processTokensHelper(
    tokens: List[SQLToken],
    stack: List[SQLToken]
  ): Option[SQLCriteria] = {
    tokens match {
      case Nil =>
        stack match {
          case (right: SQLCriteria) :: (op: SQLPredicateOperator) :: (left: SQLCriteria) :: Nil =>
            Option(
              SQLPredicate(left, op, right)
            )
          case _ =>
            stack.headOption.collect { case c: SQLCriteria => c }
        }
      case (_: StartDelimiter) :: rest =>
        val (subTokens, remainingTokens) = extractSubTokens(rest, 1)
        val subCriteria = processSubTokens(subTokens) match {
          case p: SQLPredicate => p.copy(group = true)
          case c               => c
        }
        processTokensHelper(remainingTokens, subCriteria :: stack)
      case (c: SQLCriteria) :: rest =>
        stack match {
          case (op: SQLPredicateOperator) :: (left: SQLCriteria) :: tail =>
            val predicate = SQLPredicate(left, op, c)
            processTokensHelper(rest, predicate :: tail)
          case _ =>
            processTokensHelper(rest, c :: stack)
        }
      case (op: SQLPredicateOperator) :: rest =>
        stack match {
          case (right: SQLCriteria) :: (left: SQLCriteria) :: tail =>
            val predicate = SQLPredicate(left, op, right)
            processTokensHelper(rest, predicate :: tail)
          case (right: SQLCriteria) :: (o: SQLPredicateOperator) :: tail =>
            tail match {
              case (left: SQLCriteria) :: tt =>
                val predicate = SQLPredicate(left, op, right)
                processTokensHelper(rest, o :: predicate :: tt)
              case _ =>
                processTokensHelper(rest, op :: stack)
            }
          case _ :: Nil =>
            processTokensHelper(rest, op :: stack)
          case _ =>
            throw new IllegalStateException("Invalid stack state for predicate creation")
        }
      case (_: EndDelimiter) :: rest =>
        processTokensHelper(rest, stack) // Ignore and move on
      case _ => throw new IllegalStateException("Unexpected token type")
    }
  }

  /** This method calls processTokensHelper with an empty stack (Nil) to begin processing primary
    * tokens.
    *
    * @param tokens
    * @return
    */
  private def processTokens(tokens: List[SQLToken]): Option[SQLCriteria] = {
    processTokensHelper(tokens, Nil)
  }

  /** This method is used to process subtokens extracted between delimiters. It calls
    * processTokensHelper and returns the result as a SQLCriteria, or throws an exception if no
    * criteria is found.
    *
    * @param tokens
    * @return
    */
  private def processSubTokens(tokens: List[SQLToken]): SQLCriteria = {
    processTokensHelper(tokens, Nil).getOrElse(
      throw new IllegalStateException("Empty sub-expression")
    )
  }

  /** This method is used to extract subtokens between a start delimiter (StartDelimiter) and its
    * corresponding end delimiter (EndDelimiter). It uses a recursive approach to maintain the count
    * of open and closed delimiters and correctly construct the list of extracted subtokens.
    *
    * @param tokens
    * @param openCount
    * @param subTokens
    * @return
    */
  @tailrec
  private def extractSubTokens(
    tokens: List[SQLToken],
    openCount: Int,
    subTokens: List[SQLToken] = Nil
  ): (List[SQLToken], List[SQLToken]) = {
    tokens match {
      case Nil => throw new IllegalStateException("Unbalanced parentheses")
      case (start: StartDelimiter) :: rest =>
        extractSubTokens(rest, openCount + 1, start :: subTokens)
      case (end: EndDelimiter) :: rest =>
        if (openCount - 1 == 0) {
          (subTokens.reverse, rest)
        } else extractSubTokens(rest, openCount - 1, end :: subTokens)
      case head :: rest => extractSubTokens(rest, openCount, head :: subTokens)
    }
  }
}

trait SQLCompilationError
case class SQLParserError(msg: String) extends SQLCompilationError
