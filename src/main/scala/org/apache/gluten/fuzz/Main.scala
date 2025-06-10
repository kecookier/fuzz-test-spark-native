/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gluten.fuzz

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.util.Random

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  object generateData extends Subcommand("data") {
    val numFiles: ScallopOption[Int] = opt[Int](required = true)
    val numRows: ScallopOption[Int] = opt[Int](required = true)
    val randomSeed: ScallopOption[Long] = opt[Long](required = false)
    val numColumns: ScallopOption[Int] = opt[Int](required = true)
    val excludeNegativeZero: ScallopOption[Boolean] = opt[Boolean](required = false)
  }
  addSubcommand(generateData)
  object generateQueries extends Subcommand("queries") {
    val numFiles: ScallopOption[Int] = opt[Int](required = false)
    val numQueries: ScallopOption[Int] = opt[Int](required = true)
    val randomSeed: ScallopOption[Long] = opt[Long](required = false)
  }
  addSubcommand(generateQueries)
  object runQueries extends Subcommand("run") {
    val filename: ScallopOption[String] = opt[String](required = true)
    val nativeEngine: ScallopOption[String] = opt[String](required = true)
    val numFiles: ScallopOption[Int] = opt[Int](required = false)
    val showMatchingResults: ScallopOption[Boolean] = opt[Boolean](required = false)
  }
  addSubcommand(runQueries)
  verify()
}

object Main {

  private def createSparkSession(conf: SparkConf = new SparkConf()): SparkSession = SparkSession
    .builder()
    .config(conf)
    .master("local[*]")
    .config("spark.eventLog.enabled", "false")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .config("spark.sql.warehouse.dir", "file:///opt/meituan/zhaokuo03/dev/spark-warehouse")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)
    conf.subcommand match {
      case Some(conf.generateData) =>
        val r = conf.generateData.randomSeed.toOption match {
          case Some(seed) => new Random(seed)
          case None => new Random()
        }
        DataGen.generateRandomFiles(
          r,
          createSparkSession(),
          numFiles = conf.generateData.numFiles(),
          numRows = conf.generateData.numRows(),
          numColumns = conf.generateData.numColumns(),
          generateNegativeZero = !conf.generateData.excludeNegativeZero())
      case Some(conf.generateQueries) =>
        val r = conf.generateQueries.randomSeed.toOption match {
          case Some(seed) => new Random(seed)
          case None => new Random()
        }
        QueryGen.generateRandomQueries(
          r,
          createSparkSession(),
          numFiles = conf.generateQueries.numFiles(),
          conf.generateQueries.numQueries(),
          noRandom = true)
      case Some(conf.runQueries) =>
        val nativeEngineConf = NativeEngineConf(conf.runQueries.nativeEngine())
        QueryRunner.runQueries(
          createSparkSession(nativeEngineConf.sparkConf),
          nativeEngineConf,
          conf.runQueries.numFiles(),
          conf.runQueries.filename(),
          conf.runQueries.showMatchingResults())
      case _ =>
        // scalastyle:off println
        println("Invalid subcommand")
        // scalastyle:on println
        sys.exit(-1)
    }
  }
}
