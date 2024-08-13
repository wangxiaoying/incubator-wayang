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

package org.apache.wayang.apps.tpch.data

/**
 * Represents elements from the TPC-H `PARTSUPP` table.
 */
case class PartSupp(partKey: Long,
                    suppKey: Long,
                    availqty: Long,
                    supplyCost: Double,
                    comment: String)

object PartSupp {
  val fields = IndexedSeq("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment")

  /**
   * Parse a CSV row into a [[PartSupp]] instance.
   *
   * @param csv the [[String]] to parse
   * @return the [[PartSupp]]
   */
  def parseCsv(csv: String): PartSupp = {
    val fields = csv.split("\\|")

    PartSupp(
      fields(0).toLong,
      fields(1).toLong,
      fields(2).toLong,
      fields(3).toDouble,
      fields(4)
    )
  }
}