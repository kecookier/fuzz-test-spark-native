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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable
import scala.util.Random

// 如何解决query合法性的问题？

object QueryGen {

  def generateRandomQueries(
      r: Random,
      spark: SparkSession,
      numFiles: Int,
      numQueries: Int,
      noRandom : Boolean = false): Unit = {
    for (i <- 0 until numFiles) {
      spark.read.parquet(s"zhaokuo03/test$i.parquet").createTempView(s"test$i")
    }

    val w = new BufferedWriter(new FileWriter("zhaokuo03/queries.sql"))

    val uniqueQueries = mutable.HashSet[String]()

    if (noRandom) {
      // 生成所有标量函数的 SQL
      val allFunctionQueries = generateAllScalarFunctions(r, spark, numFiles)
      println(s"Generated ${allFunctionQueries.size} queries for all scalar functions")

      // 写入文件
      allFunctionQueries.foreach { sql =>
        if (!uniqueQueries.contains(sql)) {
          uniqueQueries += sql
          w.write(sql + "\n")
        }
      }

    } else {
      for (_ <- 0 until numQueries) {
        val sql = r.nextInt().abs % 8 match {
          case _ => generateScalar(r, spark, numFiles)
          //        case 0 => generateJoin(r, spark, numFiles)
          //        case 1 => generateAggregate(r, spark, numFiles)
          //        case 2 => generateScalar(r, spark, numFiles)
          //        case 3 => generateCast(r, spark, numFiles)
          //        case 4 => generateUnaryArithmetic(r, spark, numFiles)
          //        case 5 => generateBinaryArithmetic(r, spark, numFiles)
          //        case 6 => generateBinaryComparison(r, spark, numFiles)
          //        case _ => generateConditional(r, spark, numFiles)
        }
        if (!uniqueQueries.contains(sql)) {
          uniqueQueries += sql
          w.write(sql + "\n")
        } else {
          println(s"duplicate query: $sql")
        }
      }
    }

    w.close()
  }

  private def randomChoiceFunction(functions: Seq[Function], schema: StructType, r: Random): (Function, Seq[String]) = {
    val func = Utils.randomChoice(functions, r)
    val args = func match {
      case FunctionWithSignature(_, _, _, args) =>
        println(s"func: $func")
        val fields = schema.fields
        args.map {
          case ScalarValueType(_, generateValueFunc) =>
            generateValueFunc()
          case t: WithSupportedType =>
            Utils.randomChoice(fields.filter(f => t.isSupported(f.dataType)).map(f => f.name), r)
          case argType =>
            Utils.randomChoice(fields.filter(f => f.dataType == argType).map(f => f.name), r)
        }
      case _ =>
        Range(0, func.num_args).map(_ => Utils.randomChoice(schema.fieldNames, r))
    }
    (func, args)
  }

  private def generateAggregate(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val (func, args) = randomChoiceFunction(Meta.aggFunc, table.schema, r)

    val groupingCols = Range(0, 2).map(_ => Utils.randomChoice(table.columns, r))

    if (groupingCols.isEmpty) {
      s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
        s"FROM $tableName " +
        s"ORDER BY ${args.mkString(", ")};"
    } else {
      s"SELECT ${groupingCols.mkString(", ")}, ${func.name}(${args.mkString(", ")}) " +
        s"FROM $tableName " +
        s"GROUP BY ${groupingCols.mkString(",")} " +
        s"ORDER BY ${groupingCols.mkString(", ")};"
    }
  }

  private def generateScalar(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val (func, args) = randomChoiceFunction(Meta.scalarFunc, table.schema, r)

    // Example SELECT c0, log(c0) as x FROM test0
    s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
      s"FROM $tableName " +
      s"ORDER BY ${args.mkString(", ")};"
  }

//  private def generateAllScalarFunctions(r: Random, spark: SparkSession, numFiles: Int): Seq[String] = {
//
//    val tableName = s"test${r.nextInt(numFiles)}"
//    val table = spark.table(tableName)
//    val schema = table.schema
//
//    // 遍历所有标量函数，为每个函数生成一条 SQL
//    Meta.scalarFunc.flatMap { func =>
//      try {
//        // 为每个函数尝试多次生成参数，增加成功率
//        val maxAttempts = 3
//        var attempts = 0
//        var result: Option[String] = None
//
//        while (attempts < maxAttempts && result.isEmpty) {
//          attempts += 1
//          try {
//            val args = func match {
//              case FunctionWithSignature(_, _, _, argTypes) =>
//                val fields = schema.fields
//                argTypes.map {
//                  case ScalarValueType(_, generateValueFunc) =>
//                    generateValueFunc()
//                  case t: WithSupportedType =>
//                    val supportedFields = fields.filter(f => t.isSupported(f.dataType))
//                    if (supportedFields.isEmpty) throw new RuntimeException(s"No supported fields for $t")
//                    Utils.randomChoice(supportedFields.map(_.name), r)
//                  case argType =>
//                    val matchingFields = fields.filter(f => f.dataType == argType)
//                    if (matchingFields.isEmpty) throw new RuntimeException(s"No matching fields for $argType")
//                    Utils.randomChoice(matchingFields.map(_.name), r)
//                }
//              case _ =>
//                Range(0, func.num_args).map(_ => Utils.randomChoice(schema.fieldNames, r))
//            }
//
//            // 生成 SQL
//            val sql = s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
//              s"FROM $tableName " +
//              s"ORDER BY ${args.mkString(", ")};"
//
//            result = Some(sql)
//          } catch {
//            case e: Exception =>
//              println(s"Failed to generate SQL for function ${func.name} (attempt $attempts): ${e.getMessage}")
//          }
//        }
//
//        result.toSeq
//      } catch {
//        case e: Exception =>
//          println(s"Failed to process function ${func.name}: ${e.getMessage}")
//          Seq.empty
//      }
//    }
//  }

  private def generateAllScalarFunctions(r: Random, spark: SparkSession, numFiles: Int): Seq[String] = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)
    val schema = table.schema
    val fieldNames = schema.fieldNames

    // 遍历所有标量函数
    Meta.scalarFunc.flatMap { func =>
      try {
        func match {
          case FunctionWithSignature(_, numArgs, _, argTypes) =>
            val argCombinations : Seq[Seq[String]] = argTypes.zipWithIndex.map { case (argType, idx) =>
              argType match {
                case ScalarValueType(_, generateValueFunc) =>
                  Seq(generateValueFunc())
                case t: WithSupportedType =>
                  val supportedFields = schema.fields.filter(f => t.isSupported(f.dataType))
                  if (supportedFields.isEmpty) Seq.empty else supportedFields.map(_.name).toSeq
                case specificType =>
                  val matchingFields = schema.fields.filter(f => f.dataType == specificType)
                  if (matchingFields.isEmpty) Seq.empty else matchingFields.map(_.name).toSeq
              }
            }

            if (argCombinations.exists(_.isEmpty)) {
              println(s"Skipping function ${func.name}: no matching fields for some argument types")
              Seq.empty
            } else {
              val allArgCombinations = generateCombinations(argCombinations)

              allArgCombinations.map { args =>
                s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
                  s"FROM $tableName " +
                  s"ORDER BY ${args.mkString(", ")};"
              }
            }

          case _ =>
            // 对于没有签名的函数，生成所有可能的字段组合
            val numArgs = func.num_args
            if (numArgs == 0) {
              // 无参数函数
              Seq(s"SELECT ${func.name}() AS x FROM $tableName;")
            } else {
              // 生成所有长度为numArgs的字段组合
              val fieldCombinations = generateFieldCombinations(fieldNames, numArgs)
              fieldCombinations.map { args =>
                s"SELECT ${args.mkString(", ")}, ${func.name}(${args.mkString(", ")}) AS x " +
                  s"FROM $tableName " +
                  s"ORDER BY ${args.mkString(", ")};"
              }
            }
        }
      } catch {
        case e: Exception =>
          println(s"Failed to process function ${func.name}: ${e.getMessage}")
          Seq.empty
      }
    }
  }

  // 生成参数类型的所有组合
  private def generateCombinations(argOptions: Seq[Seq[String]]): Seq[Seq[String]] = {
    argOptions.foldLeft(Seq(Seq.empty[String])) { (acc, options) =>
      for {
        combination <- acc
        option <- options
      } yield combination :+ option
    }
  }

  // 生成指定长度的字段组合（考虑到全排列可能非常大，这里限制组合数量）
  private def generateFieldCombinations(fields: Array[String], length: Int): Seq[Seq[String]] = {
    if (length == 0) {
      Seq(Seq.empty)
    } else if (length == 1) {
      fields.map(Seq(_)).toSeq
    } else {
      // 对于多参数函数，可能的组合非常多，这里限制组合数量
      val maxCombinations = 10000 // 限制组合数量，避免生成过多SQL

      def combinations(fields: Array[String], length: Int): Seq[Seq[String]] = {
        if (length == 0) {
          Seq(Seq.empty)
        } else {
          for {
            i <- fields.indices.toSeq
            rest <- combinations(fields, length - 1)
          } yield fields(i) +: rest
        }
      }

      val allCombinations = combinations(fields, length)
      if (allCombinations.length > maxCombinations) {
        println(s"Too many combinations for length $length, only generate ${maxCombinations} combinations")
        // 如果组合太多，随机选择一部分
        scala.util.Random.shuffle(allCombinations).take(maxCombinations)
      } else {
        allCombinations
      }
    }
  }

  private def generateUnaryArithmetic(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.unaryArithmeticOps, r)
    val a = Utils.randomChoice(table.columns, r)

    // Example SELECT a, -a FROM test0
    s"SELECT $a, $op$a " +
      s"FROM $tableName " +
      s"ORDER BY $a;"
  }

  private def generateBinaryArithmetic(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.binaryArithmeticOps, r)
    val a = Utils.randomChoice(table.columns, r)
    val b = Utils.randomChoice(table.columns, r)

    // Example SELECT a, b, a+b FROM test0
    s"SELECT $a, $b, $a $op $b " +
      s"FROM $tableName " +
      s"ORDER BY $a, $b;"
  }

  private def generateBinaryComparison(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.comparisonOps, r)
    val a = Utils.randomChoice(table.columns, r)
    val b = Utils.randomChoice(table.columns, r)

    // Example SELECT a, b, a <=> b FROM test0
    s"SELECT $a, $b, $a $op $b " +
      s"FROM $tableName " +
      s"ORDER BY $a, $b;"
  }

  private def generateConditional(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val op = Utils.randomChoice(Meta.comparisonOps, r)
    val a = Utils.randomChoice(table.columns, r)
    val b = Utils.randomChoice(table.columns, r)

    // Example SELECT a, b, IF(a <=> b, 1, 2), CASE WHEN a <=> b THEN 1 ELSE 2 END FROM test0
    s"SELECT $a, $b, $a $op $b, IF($a $op $b, 1, 2), CASE WHEN $a $op $b THEN 1 ELSE 2 END " +
      s"FROM $tableName " +
      s"ORDER BY $a, $b;"
  }

  private def generateCast(r: Random, spark: SparkSession, numFiles: Int): String = {
    val tableName = s"test${r.nextInt(numFiles)}"
    val table = spark.table(tableName)

    val toType = Utils.randomWeightedChoice(Meta.dataTypes, r).sql
    val arg = Utils.randomChoice(table.columns, r)

    // We test both `cast` and `try_cast` to cover LEGACY and TRY eval modes. It is not
    // recommended to run Comet Fuzz with ANSI enabled currently.
    // Example SELECT c0, cast(c0 as float), try_cast(c0 as float) FROM test0
    s"SELECT $arg, cast($arg as $toType), try_cast($arg as $toType) " +
      s"FROM $tableName " +
      s"ORDER BY $arg;"
  }

  private def generateJoin(r: Random, spark: SparkSession, numFiles: Int): String = {
    val leftTableName = s"test${r.nextInt(numFiles)}"
    val rightTableName = s"test${r.nextInt(numFiles)}"
    val leftTable = spark.table(leftTableName)
    val rightTable = spark.table(rightTableName)

    val leftCol = Utils.randomChoice(leftTable.columns, r)
    val rightCol = Utils.randomChoice(rightTable.columns, r)

    val joinTypes = Seq(("INNER", 0.4), ("LEFT", 0.3), ("RIGHT", 0.3))
    val joinType = Utils.randomWeightedChoice(joinTypes, r)

    val leftColProjection = leftTable.columns.map(c => s"l.$c").mkString(", ")
    val rightColProjection = rightTable.columns.map(c => s"r.$c").mkString(", ")
    "SELECT " +
      s"$leftColProjection, " +
      s"$rightColProjection " +
      s"FROM $leftTableName l " +
      s"$joinType JOIN $rightTableName r " +
      s"ON l.$leftCol = r.$rightCol " +
      "ORDER BY " +
      s"$leftColProjection, " +
      s"$rightColProjection;"
  }

}

trait FunctionTrait {
  def name: String
  def num_args: Int
}

class Function(override val name: String, override val num_args: Int) extends FunctionTrait

object Function {
  def apply(name: String, num_args: Int): Function = new Function(name, num_args)
}

case class FunctionWithSignature(
    override val name: String,
    override val num_args: Int,
    ret: DataType,
    args: Seq[DataType]) extends Function(name, num_args)

trait FuzzDataType {
  def defaultSize: Int = throw new UnsupportedOperationException("defaultSize is not supported")
  def asNullable: DataType = throw new UnsupportedOperationException("asNullable is not supported")
}

trait WithSupportedType {
  def isSupported(dataType: DataType): Boolean
}

case class ScalarValueType(valueType: DataType, generateValueFunc: () => String)
  extends DataType with FuzzDataType

case class MultipleDataTypes(dataTypes: Seq[DataType]) extends DataType with FuzzDataType with WithSupportedType {
  override def isSupported(dataType: DataType): Boolean = dataTypes.contains(dataType)
}

case class AllDataTypes() extends DataType with FuzzDataType with WithSupportedType {
  override def isSupported(dataType: DataType): Boolean = true
}
