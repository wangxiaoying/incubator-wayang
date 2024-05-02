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
import org.apache.wayang.apps.tpch.data.{Customer, LineItem, Order, Region, Nation}
import org.apache.wayang.apps.util.ExperimentDescriptor
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.jdbc.operators.JdbcTableSource
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate

/**
  * Apache Wayang (incubating) implementation of Simple test query.
  *
  * {{{
  * select
  *   n_name,
  *   r_name
  * from
  *   nation,
  *   region
  * where
  *   n_regionkey = r_regionkey
  *   and n_nationkey > 3;
  * }}}
  */
class QueryTest(plugins: Plugin*) extends ExperimentDescriptor {

  override def version = "0.1.0"

  def apply(configuration: Configuration,
            jdbcPlatform: JdbcPlatformTemplate,
            createTableSource: (String, Seq[String]) => JdbcTableSource)
           (implicit experiment: Experiment) = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
      .withJobName(s"TPC-H (${this.getClass.getSimpleName})")
      .withUdfJarsOf(classOf[QueryTest])
      .withExperiment(experiment)

    val schema = configuration.getOptionalStringProperty("wayang.apps.tpch.schema").orElse(null)
    def withSchema(table: String) = schema match {
      case null => table
      case str: String => s"$str.$table"
    }

    experiment.getSubject.addConfiguration("jdbcUrl", configuration.getStringProperty(jdbcPlatform.jdbcUrlProperty))
    if (schema != null) experiment.getSubject.addConfiguration("schema", schema)

    // Read, filter, and project the nation data.
    val nation = planBuilder
      .readTable(createTableSource(withSchema("NATION"), Nation.fields))
      .withName("Load NATION table")

      .filter(_.getLong(0) > 3, sqlUdf = "n_nationkey > 3", selectivity = .25)
      .withName("Filter nations")

      .projectRecords(Seq("n_regionkey", "n_name"))
      .withName("Project customers")

      .map(nation => (nation.getLong(0),
            nation.getString(1)))
      .withName("Extract nation key and name")

    // Read, filter, and project the region data.
    val region = planBuilder
      .load(createTableSource(withSchema("region"), Region.fields))
      .withName("Load REGION table")

      .projectRecords(Seq("r_regionkey", "r_name"))
      .withName("Project region")

      .map(region => (region.getLong(0),
        region.getString(1))
      )
      .withName("Unpack region")

    // Join and aggregate the different datasets.
    nation
      .join[(Long, String), Long](_._1, region, _._1)
      .withName("Join nation with region")

      .map(coli => QueryTest.Result(coli.field0._2, coli.field1._2))
      .withName("Project CO-line-item join product")

      .collect(true)
  }

}

object QueryTest {

  case class Result(n_name: String,
                    r_name: String)

}
