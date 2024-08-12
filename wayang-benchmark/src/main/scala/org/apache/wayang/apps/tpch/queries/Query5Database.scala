/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.tpch.queries

import org.apache.wayang.api._
import org.apache.wayang.apps.tpch.CsvUtils
import org.apache.wayang.apps.tpch.data.{Customer, LineItem, Order, Supplier, Nation, Region}
import org.apache.wayang.apps.util.ExperimentDescriptor
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.jdbc.operators.JdbcTableSource
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate

/**
  * Apache Wayang (incubating) implementation of TPC-H Query 5.
  *
  * {{{
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = [REGION]
    AND o_orderdate >= CAST('[DATE]' AS date)
    AND o_orderdate < CAST('[DATE2]' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC
  * }}}
  */
class Query5Database(plugins: Plugin*) extends ExperimentDescriptor {

  override def version = "0.1.0"

  def apply(configuration: Configuration,
            jdbcPlatform: JdbcPlatformTemplate,
            createTableSource: (String, Seq[String]) => JdbcTableSource,
            pRegion: String = "ASIA",
            date: String = "1994-01-01",
            date2: String = "1995-01-01")
           (implicit experiment: Experiment) = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
      .withJobName(s"TPC-H (${this.getClass.getSimpleName})")
      .withUdfJarsOf(classOf[Query5Database])
      .withExperiment(experiment)

    val schema = configuration.getOptionalStringProperty("wayang.apps.tpch.schema").orElse(null)
    def withSchema(table: String) = schema match {
      case null => table
      case str: String => s"$str.$table"
    }

    experiment.getSubject.addConfiguration("jdbcUrl", configuration.getStringProperty(jdbcPlatform.jdbcUrlProperty))
    if (schema != null) experiment.getSubject.addConfiguration("schema", schema)
    experiment.getSubject.addConfiguration("region", pRegion)
    experiment.getSubject.addConfiguration("date", date)
    experiment.getSubject.addConfiguration("date2", date2)

    val _region = pRegion
    val _date = CsvUtils.parseDate(date)
    val _date2 = CsvUtils.parseDate(date2)

    // Read, filter, and project the customer data.
    val customerKeys = planBuilder
      .readTable(createTableSource(withSchema("CUSTOMER"), Customer.fields))
      .withName("Load CUSTOMER table")

      .projectRecords(Seq("c_custkey", "c_nationkey"))
      .withName("Project customers")

      .map(customer => (customer.getLong(0), // c_custkey
        customer.getLong(1))) // c_nationkey
      .withName("Extract customer ID and nationkey")

    // Read, filter, and project the order data.
    val orders = planBuilder
      .load(createTableSource(withSchema("ORDERS"), Order.fields))
      .withName("Load ORDERS table")

      .filter(t => CsvUtils.parseDate(t.getString(4)) >= _date && CsvUtils.parseDate(t.getString(4)) < _date2, sqlUdf = s"o_orderdate >= date('$date') and o_orderdate < date('$date2')")
      .withName("Filter orders")

      .projectRecords(Seq("o_orderkey", "o_custkey"))
      .withName("Project orders")

      .map(order => (order.getLong(0), // orderKey
        order.getLong(1)) // custKey
      )
      .withName("Unpack orders")

    // Read, filter, and project the lineitem data.
    val lineItems = planBuilder
      .readTable(createTableSource(withSchema("LINEITEM"), LineItem.fields))
      .withName("Load LINEITEM table")

      .projectRecords(Seq("l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"))
      .withName("Project line items")

      .map(li => (
        li.getLong(0), //li.orderKey,
        li.getLong(1), // li.suppKey
        li.getDouble(2) * (1 - li.getDouble(3)) //li.extendedPrice * (1 - li.discount) = revenue
      ))
      .withName("Extract line item data")

    // Read, filter and project the supplier data.
    val supplier = planBuilder
      .readTable(createTableSource(withSchema("SUPPLIER"), Supplier.fields))
      .withName("Load SUPPLIER table")

      .projectRecords(Seq("s_suppkey", "s_nationkey"))
      .withName("project supplier")

      .map(supplier => (supplier.getLong(0), // s_suppkey
        supplier.getLong(1))) // s_nationkey
      .withName("Extract supplier data")

    // Read, filter and project nation data.
    val nation = planBuilder
      .readTable(createTableSource(withSchema("NATION"), Nation.fields))
      .withName("Load NATION table")

      .projectRecords(Seq("n_nationkey", "n_name", "n_regionkey"))
      .withName("project nation")

      .map(nation => (nation.getLong(0),
        nation.getString(1),
        nation.getLong(2)))
      .withName("Extract nation data")

    // Read, filter and project region data.
    val region = planBuilder
      .readTable(createTableSource(withSchema("REGION"), Region.fields))
      .withName("Load REGION table")

      .filter(t => t.getString(1) == _region, sqlUdf = s"r_name = '$pRegion'")
      .withName("Filter region")

      .projectRecords(Seq("r_regionkey"))
      .withName("project region")

      .map(region => region.getLong(0)) // r_regionkey
      .withName("Extract region data")

    // Join and aggregate the different datasets.
    customerKeys
      .join[(Long, Long), Long](_._1, orders, _._2)
      .withName("Join customers with orders") // c_custkey = o_custkey

      .map(t => (t.field0._2, t.field1._1)) // (c_nationkey, o_orderkey)
      .withName("Project customer-order join product")

      .join[(Long, Long, Double), Long](_._2, lineItems, _._1) // o_orderkey = l_orderkey
      .withName("Join CO with line items")

      .map(t => (t.field0._1, t.field1._2, t.field1._3)) // (c_nationkey, l_suppkey, revenue)
      .withName("Project CO-line-item join product")

      .join[(Long, Long), (Long, Long)](t => (t._2, t._1), supplier, t => (t._1, t._2)) // l_suppkey = s_suppkey and c_nationkey = s_nationkey
      .withName("Join COL with supplier")

      .map(t => (t.field0._3, t.field1._2)) // (revenue, s_nationkey)_
      .withName("Project COLS join product")

      .join[(Long, String, Long), Long](_._2, nation, _._1) // s_nationkey = n_nationkey
      .withName ("Join COLS with nation")

      .map(t => (t.field0._1, t.field1._2, t.field1._3)) // (revenue, n_name, n_regionkey)
      .withName("Project COLSN join product")

      .join[Long, Long](_._3, region, identity) // n_reionkey = r_regionkey
      .withName("Join CLOSN with region")

      .map(t => Query5Result(t.field0._2, t.field0._1))
      .withName("Project COLSNR join product")

      .reduceByKey(
        t => t.name,
        (t1, t2) => {
          t1.revenue += t2.revenue;
          t1
        }
      )
      .withName("Aggregate revenue")

      .sort[Double](t => t.revenue)
      .withName("Sort on revenue")

      .collect()
  }

}


