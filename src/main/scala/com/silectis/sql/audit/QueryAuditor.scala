package com.silectis.sql.audit

import com.silectis.sql.ColumnAlias
import com.silectis.sql.ColumnReference
import com.silectis.sql.ColumnExpression
import com.silectis.sql.SqlFunction
import com.silectis.sql.Subquery
import com.silectis.sql.TableAlias
import com.silectis.sql.TableExpression
import com.silectis.sql.TableReference
import com.silectis.sql.parse.SqlParser
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try
import com.silectis.sql.QueryColumn

class QueryAuditor extends LazyLogging {
  private val parser = new SqlParser

  private def parseTable(
      from: Option[TableExpression],
      columnsBuffer: scala.collection.mutable.ArrayBuffer[String]
  ): Option[TableReference] = {
    from match {
      case Some(ref: TableReference) => Some(ref)
      case Some(alias: TableAlias)   => Some(alias.table)
      case Some(subquery: Subquery) =>
        parseColumns(subquery.query.columns, Some(subquery), columnsBuffer)
        parseTable(subquery.query.from, columnsBuffer)
      case None => None
    }
  }

  private def parseColumns(
      columns: Seq[QueryColumn],
      from: Option[TableExpression],
      columnsBuffer: scala.collection.mutable.ArrayBuffer[String]
  ): Unit = {
    from match {
      case Some(_: TableReference) =>
        columns.foreach {
          case ColumnReference(columnName) => columnsBuffer += columnName
          case ColumnAlias(expr, _) =>
            extractColumnName(expr).foreach(columnsBuffer += _)
          case _ =>
        }

      case Some(alias: TableAlias) =>
        parseColumns(columns, Some(alias.table), columnsBuffer)

      case Some(subquery: Subquery) =>
        val innerColumnsBuffer = scala.collection.mutable.ArrayBuffer[String]()
        parseColumns(
          subquery.query.columns,
          subquery.query.from,
          innerColumnsBuffer
        )

        columns.foreach {
          case ColumnAlias(expr, _) =>
            extractColumnName(expr)
              .filter(innerColumnsBuffer.contains)
              .foreach(columnsBuffer += _)
          case ColumnReference(columnName)
              if innerColumnsBuffer.contains(columnName) =>
            columnsBuffer += columnName
          case _ =>
        }

      case None =>
    }
  }

  private def extractColumnName(expr: ColumnExpression): Option[String] = {
    expr match {
      case ColumnReference(columnName) => Some(columnName)
      case _                           => None
    }
  }

  /** Audit a SQL query.
    *
    * @param sql
    *   The sql string to audit.
    * @return
    *   A QueryAuditResult with the schema, table, and columns (assuming any
    *   exist).
    */
  def audit(sql: String): Try[QueryAuditResult] = {
    Try(parser.parse(sql))
      .map { query =>
        logger.debug(s"""Parsed query "$sql" as $query""")

        val columnsBuffer = scala.collection.mutable.ArrayBuffer[String]()

        var table = parseTable(query.from, columnsBuffer)
        parseColumns(query.columns, query.from, columnsBuffer)

        new QueryAuditResult(table, columnsBuffer.distinct.toSeq)
      }
  }
}
