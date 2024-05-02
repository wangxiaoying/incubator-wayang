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

package org.apache.wayang.apps.tpch

import org.apache.wayang.apps.tpch.queries.{Query1, Query3Database, Query3File, Query3Hybrid, QueryTest, QueryTest2}
import org.apache.wayang.apps.util.{Parameters, ProfileDBHelper, StdOut}
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.postgres.operators.PostgresTableSource
import org.apache.wayang.sqlite3.Sqlite3
import org.apache.wayang.sqlite3.operators.Sqlite3TableSource


/**
  * This app adapts some TPC-H queries.
  */
object TpcH {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println(s"Usage: <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <TPC-H config URL> <query> [<query args>*]")
      sys.exit(1)
    }

    val experimentArg = args(0)
    val plugins = Parameters.loadPlugins(args(1))
    val configUrl = args(2)
    val queryName = args(3)

    val jdbcPlatform = {
      val jdbcPlatforms = plugins
        .flatMap(
          plugin => {
            val list = plugin.getRequiredPlatforms
            var array: List[Platform] = List[Platform]()
            val iterator = list.iterator()
            while(iterator.hasNext){
              array :+= iterator.next()
            }
            array
          }
        )
        .filter(_.isInstanceOf[JdbcPlatformTemplate])
        .distinct
      if (jdbcPlatforms.size == 1) jdbcPlatforms.head.asInstanceOf[JdbcPlatformTemplate]
      else if (jdbcPlatforms.isEmpty) null
      else throw new IllegalArgumentException(s"Detected multiple databases: ${jdbcPlatforms.mkString(", ")}.")
    }

    val createTableSource =
      if (jdbcPlatform == null) {
        (table: String, columns: Seq[String]) => throw new IllegalStateException("No database plugin detected.")
      } else if (jdbcPlatform.equals(Sqlite3.platform)) {
        (table: String, columns: Seq[String]) => new Sqlite3TableSource(table, columns: _*)
      } else if (jdbcPlatform.equals(Postgres.platform)) {
        (table: String, columns: Seq[String]) => new PostgresTableSource(table, columns: _*)
      } else {
        throw new IllegalArgumentException(s"Unsupported database: $jdbcPlatform.")
      }


    println(configUrl)
    val configuration = new Configuration(configUrl)

    var experiment: Experiment = null
    queryName match {
      case "Test" =>
        val query = new QueryTest(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val start = System.currentTimeMillis
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        val end = System.currentTimeMillis
        StdOut.printLimited(result, 10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case "Test2" =>
        val query = new QueryTest2(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val start = System.currentTimeMillis
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        val end = System.currentTimeMillis
        StdOut.printLimited(result, 10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case "Q1" =>
        val query = new Query1(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val start = System.currentTimeMillis
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        val end = System.currentTimeMillis
        StdOut.printLimited(result, 10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case "Q3File" =>
        val query = new Query3File(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val start = System.currentTimeMillis
        val result = query(configuration)(experiment)
        val end = System.currentTimeMillis
        StdOut.printLimited(result, 10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case "Q3" =>
        val query = new Query3Database(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val start = System.currentTimeMillis
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        val end = System.currentTimeMillis
        StdOut.printLimited(result, 10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case "Q3Hybrid" =>
        val query = new Query3Hybrid(plugins: _*)
        experiment = Parameters.createExperiment(experimentArg, query)
        experiment.getSubject.addConfiguration("plugins", args(1))
        experiment.getSubject.addConfiguration("query", args(3))
        val start = System.currentTimeMillis
        val result = query(configuration, jdbcPlatform, createTableSource)(experiment)
        val end = System.currentTimeMillis
        StdOut.printLimited(result, 10)
        val elapsed = end - start;
        println(s"Time in total (ms): $elapsed")
      case other: String => {
        println(s"Unknown query: $other")
        sys.exit(1)
      }
    }

    // Store experiment data.
    ProfileDBHelper.store(experiment, configuration)
  }

}
