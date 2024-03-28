package org.apache.wayang.apps.tpch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.wayang.apps.tpch.queries.{Query1, Query3Database, Query3File, Query3Hybrid, QueryTest, QueryTest2}
import org.apache.wayang.apps.util.{Parameters, ProfileDBHelper, StdOut}
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.Configuration

object SparkTpcH {
  private val TPCH_Q3 =
    """SELECT
      l_orderkey,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      o_orderdate,
      o_shippriority
  FROM
      customer,
      orders,
      lineitem
  WHERE
      c_mktsegment = 'BUILDING'
      AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate < CAST('1995-03-15' AS date)
      AND l_shipdate > CAST('1995-03-15' AS date)
  GROUP BY
      l_orderkey,
      o_orderdate,
      o_shippriority
  ORDER BY
      revenue DESC,
      o_orderdate"""

  private val POSTGRES_USER = "postgres"
  private val POSTGRES_PASSWORD = "postgres"
  private val POSTGRES_URL = "jdbc:postgresql://10.155.96.80:5432/tpch"

  def registerPostgres(spark: SparkSession, tables: Array[String], url: String): Unit = {
    tables.foreach(name => {
      spark.sql(
        s"""
                CREATE TEMPORARY VIEW $name
                USING org.apache.spark.sql.jdbc
                OPTIONS (
                  driver "org.postgresql.Driver",
                  url "$url",
                  dbtable "public.$name",
                  user '$POSTGRES_USER',
                  password '$POSTGRES_PASSWORD',
                  pushDownAggregate 'true'
                )
                """)
    })
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println(s"Usage: <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <TPC-H config URL> <query> [<query args>*]")
      sys.exit(1)
    }

    val experimentArg = args(0)
    val plugins = Parameters.loadPlugins(args(1))
    val configUrl = args(2)
    val queryName = args(3)

    println(configUrl)
    val configuration = new Configuration(configUrl)

    val spark = SparkSession.builder().master("local[4]")
      .appName("test-spark")
      .config("spark.executor.memory", "110g")
      .config("spark.driver.memory", "110g")
      .config("spark.log.level", "INFO")
      .config("spark.ui.port", "4040")
      .config("spark.sql.cbo.enabled", "true")
      .config("", "true")
      .getOrCreate()

    registerPostgres(spark, Array("lineitem", "customer", "orders", "nation", "region", "supplier", "part", "partsupp"), POSTGRES_URL)

    var experiment: Experiment = null
    queryName match {
      case "Q3" =>
        val start = System.currentTimeMillis
        val df = spark.sql(TPCH_Q3)
        df.collect()
        val end = System.currentTimeMillis
        df.show(10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case other: String => {
        println(s"Unknown query: $other")
        sys.exit(1)
      }
    }

  }
}
