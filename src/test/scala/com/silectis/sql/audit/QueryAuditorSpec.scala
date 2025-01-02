package com.silectis.sql.audit

import com.silectis.sql.SqlException
import com.silectis.sql.TableExpression
import com.silectis.sql.TableReference
import com.silectis.test.UnitSpec
import scala.util.Success

/** @author
  *   jlouns
  */
class QueryAuditorSpec extends UnitSpec {
  val auditor = new QueryAuditor

  it should "return failure for an invalid SQL query" in {
    a[SqlException] should be thrownBy
      auditor.audit("not valid sql").get
  }

  it should "return the correct query from a table" in {
    val parsed = auditor.audit("select col1, col2 from table1")
    parsed shouldBe Success(
      QueryAuditResult(
        Some(TableReference(None, "table1")),
        List("col1", "col2")
      )
    )
  }

  it should "return the correct query from a table with a schema" in {
    val parsed = auditor.audit("SELECT col1 FROM schema1.table1")
    parsed shouldBe Success(
      QueryAuditResult(
        Some(TableReference(Some("schema1"), "table1")),
        List("col1")
      )
    )
  }

  it should "return the correct query with no table and no columns" in {
    val parsed = auditor.audit("select 1 as one, 'two'")
    parsed shouldBe Success(
      QueryAuditResult(
        None,
        List()
      )
    )
  }

  it should "return the correct query with a column expression non-nested subquery" in {
    val parsed = auditor.audit("SELECT 1, upper('test') AS name FROM table1")
    parsed shouldBe Success(
      QueryAuditResult(Some(TableReference(None, "table1")), List())
    )
  }

  it should "return the correct query with a subquery and aliases" in {
    val parsed = auditor.audit("""
    select a, b, x, y from (
      select
        col1 as a,
        2 as b,
        x,
        upper('test') as y
      from table2
    ) as s
    """)
    parsed shouldBe Success(
      QueryAuditResult(
        Some(TableReference(None, "table2")),
        List("col1", "x")
      )
    )
  }

  it should "return the correct query with a nested subquery" in {
    val parsed = auditor.audit("""
    SELECT x, y FROM (
      SELECT a AS x, b AS y FROM (
        SELECT
          col1 AS a,
          col2 AS b
        FROM table3
      ) AS inner
    ) AS outer
    """)
    parsed shouldBe Success(
      QueryAuditResult(
        Some(TableReference(None, "table3")),
        List("col1", "col2")
      )
    )
  }
}
