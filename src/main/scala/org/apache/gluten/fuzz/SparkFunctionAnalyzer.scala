package org.apache.gluten.fuzz
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

import java.io.{File, PrintWriter}
import org.apache.spark.sql.types._

import scala.io.Source

object SparkFunctionAnalyzer {

  def main(args: Array[String]): Unit = {
    // 解析命令行参数
//    val detail = args.contains("--detail")

////     创建SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Function Analyzer")
      .config(new SparkConf())
      .master("local[*]")
      .config("spark.eventLog.enabled", "false")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.sql.warehouse.dir", "file:///opt/meituan/zhaokuo03/dev/spark-warehouse")
      .getOrCreate()

//    genValidSql()
    try {
      // gen func_args
//      extractFuncMetaFile(spark);

      //
//      genTestSql()

      //
//      genValidSql()


      val functions = getBuiltinFunctions(spark)
      functions.foreach { case (exprInfo, clazz) =>
        val name = exprInfo.getName
        val group = exprInfo.getGroup match {
          case "" => "empty"
          case x => x
        }
        println(s"$name $group")
      }
//      GenerateFunctionArgTypes.writeFuncMetaToFile(functionMetas, "func_meta")


    } finally {
      spark.stop()
    }
  }

  def extractFuncMetaFile(spark: SparkSession): Unit = {
    val outputFile = "func_args"
    // 获取内置函数列表
    val functions = getBuiltinFunctions(spark)

    // 创建输出文件
    val writer = new PrintWriter(new File(outputFile))

    try {
      println(s"Spark SQL 内置函数总数: ${functions.size}")
      println("=" * 50)
      writer.println("FunctionName:ParamCount:ParamTypes:Group:ClassName")
      // 分析并打印函数信息
      functions.sortBy(_._1.getName).foreach { case (exprInfo, clazz) =>
        analyzeFunctionParameters(exprInfo, clazz, writer)
      }
      println(s"分析结果已输出到文件: $outputFile")
    } finally {
      writer.close()
    }
  }

  def genTestSql(): Unit = {
    val functionMetas = genFunctionMeta()
    println(s"{ $functionMetas }")

    val sqlDir = new File("./sql")
    if (!sqlDir.exists()) {
      sqlDir.mkdirs()
    }

    for (meta <- functionMetas) {
      val sql = genOnFunctionSql(meta)
      // sql写到文件 meta.name.sql 里
      // sql Seq[string]里每个string一行
      val sqlFile = s"${meta.name}.sql"
      val writer = new PrintWriter(new File(sqlDir, sqlFile))
      writer.println(sql.mkString("\n"))
      writer.close()
      println(s"SQL 语句已输出到文件: ./sql/$sqlFile count: ${sql.size}")
    }
  }

  def genValidSql(): Unit = {
    val functionMetas = genFunctionMeta()
    GenerateFunctionArgTypes.validAllFunctions(functionMetas)
  }

  /**
   * 获取Spark SQL内置函数列表
   */
  def getBuiltinFunctions(spark: SparkSession): Seq[(ExpressionInfo, Class[_])] = {
    val functionRegistry = FunctionRegistry
    // 获取函数映射
    val expressions = functionRegistry.expressions
    println(s"Spark SQL all builtin funtions count: ${expressions.size}")

    // 提取函数名称和类信息
    expressions.toSeq.map { case (_, (exprInfo, _)) =>
      val className = exprInfo.getClassName
      try {
        (exprInfo, Class.forName(className))
      } catch {
        case _: ClassNotFoundException =>
          println(s"类 $className 未找到")
          (exprInfo, null)
      }
    }.filter(_._2 != null)
  }

  /**
   * 分析函数参数
   */

  def analyzeFunctionParameters(expressionInfo: ExpressionInfo,  clazz: Class[_], writer: PrintWriter): Unit = {
    val name = expressionInfo.getName
    val group = expressionInfo.getGroup match {
      case "" => "unknown_group"
      case x => x
    }
    // 获取所有构造函数
    val constructors = clazz.getConstructors

    // 对于RuntimeReplaceable，跳过参数最多的构造函数
    val effectiveConstructors = if (classOf[RuntimeReplaceable].isAssignableFrom(clazz)) {
      val all = constructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      constructors
    }

    // 查找接受 Seq[Expression] 的可变参数构造函数
    val varargCtor = effectiveConstructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))

    if (varargCtor.isDefined) {
      val paramCount = -1
      val paramTypes = varargCtor.head.getParameterTypes.map(_.getSimpleName).mkString("; ")
      writer.println(s"""$name:$paramCount:($paramTypes):$group:${clazz.getName}""")
    } else {
      // 获取所有构造函数的参数个数
      val validParametersCount = effectiveConstructors
        .map(_.getParameterCount).distinct.sorted

//      effectiveConstructors.foreach { ctor =>
//        val paramCount = ctor.getParameterCount
//        val paramTypes = ctor.getParameterTypes.map(_.getSimpleName).mkString(", ")
//        writer.println(s"""$name:$paramCount:($paramTypes):$group:${clazz.getName}""")
//      }
      effectiveConstructors.foreach { ctor =>
        val paramCount = ctor.getParameterCount
        val paramTypes = ctor.getParameterTypes.zipWithIndex.map { case (clazz, idx) =>
          if (clazz.getSimpleName == "Map") {
            // 获取泛型参数类型
            val genTypes = ctor.getGenericParameterTypes
            if (genTypes.length > idx) {
              genTypes(idx) match {
                case p: java.lang.reflect.ParameterizedType =>
                  val typeArgs = p.getActualTypeArguments
                  if (typeArgs.length == 2) {
                    val keyType = typeArgs(0).getTypeName.split("\\.").last
                    val valueType = typeArgs(1).getTypeName.split("\\.").last
                    s"Map<$keyType, $valueType>"
                  } else {
                    "Map<?, ?>"
                  }
                case _ => "Map<?, ?>"
              }
            } else {
              "Map<?, ?>"
            }
          }  else if (clazz.getSimpleName == "StructType") {
            val genTypes = ctor.getGenericParameterTypes
            if (genTypes.length > idx) {
              genTypes(idx) match {
                case p: java.lang.reflect.ParameterizedType =>
                  val typeArgs = p.getActualTypeArguments
                  val memberTypes = typeArgs.map(_.getTypeName.split("\\.").last).mkString(", ")
                  s"Struct<$memberTypes>"
                case _ => "Struct<?>"
              }
            } else {
              "Struct<?>"
            }
          }
          else {
            clazz.getSimpleName
          }
        }.mkString("; ")
        writer.println(s"""$name:$paramCount:($paramTypes):$group:${clazz.getName}""")
      }

      if (effectiveConstructors.isEmpty) {
        throw new RuntimeException(s"函数 $name 没有有效的构造函数")
      }
    }
  }



  val dataTypesValues: Seq[(DataType, Seq[Any])] = Seq(
    (DataTypes.BooleanType, Seq(true, false, null)),

    (DataTypes.ByteType, Seq(Byte.MinValue, Byte.MaxValue, 0.toByte, 1.toByte, -1.toByte, null)),
    (DataTypes.ShortType, Seq(Short.MinValue, Short.MaxValue, 0.toShort, 1.toShort, -1.toShort, null)),
    (DataTypes.IntegerType, Seq(Int.MinValue, Int.MaxValue, 0, 1, -1, null)),
    (DataTypes.LongType, Seq(Long.MinValue, Long.MaxValue, 0L, 1L, -1L, null)),

    (DataTypes.FloatType, Seq(Float.MinValue, Float.MaxValue, Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN, 0.0f, 1.0f, -1.0f, null)),
    (DataTypes.DoubleType, Seq(Double.MinValue, Double.MaxValue, Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN, 0.0, 1.0, -1.0, null)),

    (DataTypes.createDecimalType(10, 2), Seq(
      java.math.BigDecimal.valueOf(99999999.99),
      java.math.BigDecimal.valueOf(-99999999.99),
      java.math.BigDecimal.ZERO,
      java.math.BigDecimal.ONE,
      java.math.BigDecimal.valueOf(-1),
      null)),

    (DataTypes.DateType, Seq(
      java.sql.Date.valueOf("1900-01-01"),
      java.sql.Date.valueOf("9999-12-31"),
      java.sql.Date.valueOf("1970-01-01"),
      java.sql.Date.valueOf("2000-01-01"),
      null)),
    (DataTypes.TimestampType, Seq(
      java.sql.Timestamp.valueOf("1900-01-01 00:00:00"),
      java.sql.Timestamp.valueOf("2038-01-19 03:14:07"),
      java.sql.Timestamp.valueOf("1970-01-01 00:00:00"),
      java.sql.Timestamp.valueOf("2000-01-01 00:00:00"),
      null)),

    (DataTypes.StringType, Seq("", "a", "测试", "very long string " * 100, "\n\t\r", "null", null)),
    (DataTypes.BinaryType, Seq(Array.empty[Byte], Array(0.toByte), Array(255.toByte), Array.fill(1000)(1.toByte), null))
  )



  def genOnFunctionSql(func: FunctionMeta): Seq[String] = {
    val funcName = func.name
    val argTypes = func.argTypes

    // 获取每个参数的候选值列表
    val argValues: Seq[Seq[Any]] = argTypes.map {
      case "Expression" | "DataType" =>
        dataTypesValues.flatMap(_._2)
      case "boolean" =>
        dataTypesValues.find(_._1 == DataTypes.BooleanType).map(_._2).getOrElse(Seq("null"))
      case "byte" =>
        dataTypesValues.find(_._1 == DataTypes.ByteType).map(_._2).getOrElse(Seq("null"))
      case "short" =>
        dataTypesValues.find(_._1 == DataTypes.ShortType).map(_._2).getOrElse(Seq("null"))
      case "int" | "integer" =>
        dataTypesValues.find(_._1 == DataTypes.IntegerType).map(_._2).getOrElse(Seq("null"))
      case "long" =>
        dataTypesValues.find(_._1 == DataTypes.LongType).map(_._2).getOrElse(Seq("null"))
      case "float" =>
        dataTypesValues.find(_._1 == DataTypes.FloatType).map(_._2).getOrElse(Seq("null"))
      case "double" =>
        dataTypesValues.find(_._1 == DataTypes.DoubleType).map(_._2).getOrElse(Seq("null"))
      case "decimal" =>
        dataTypesValues.find(_._1.isInstanceOf[DecimalType]).map(_._2).getOrElse(Seq("null"))
      case "date" =>
        dataTypesValues.find(_._1 == DataTypes.DateType).map(_._2).getOrElse(Seq("null"))
      case "timestamp" =>
        dataTypesValues.find(_._1 == DataTypes.TimestampType).map(_._2).getOrElse(Seq("null"))
      case "string" =>
        dataTypesValues.find(_._1 == DataTypes.StringType).map(_._2).getOrElse(Seq("null"))
      case "binary" =>
        dataTypesValues.find(_._1 == DataTypes.BinaryType).map(_._2).getOrElse(Seq("null"))
      case other =>
        throw new IllegalArgumentException(s"未知参数类型: $other")
    }

    // 计算所有参数的笛卡尔积
    val argCombos: Seq[Seq[Any]] = argValues.foldLeft(Seq(Seq.empty[Any])) {
      (acc, values) => for (a <- acc; v <- values) yield a :+ v
    }

    // 拼接成SQL
    argCombos.map { args =>
      val argStr = args.map {
        case s: String => s"'$s'"
        case null => "null"
        case arr: Array[Byte] => "X'" + arr.map("%02X".format(_)).mkString + "'"
        case v => v.toString
      }.mkString(", ")

      s"select $funcName($argStr)"
    }
  }

  // 示例：解析 func_args 文件，生成 FunctionMeta 列表，忽略第一行
  def genFunctionMeta(): Seq[FunctionMeta] = {
    val source = Source.fromFile("func_args")
    try {
      // 立即读取所有行到内存，避免流关闭后再访问
      val lines = source.getLines().toList.drop(1)
      lines.map { line =>
        val parts = line.split(":", 5)
        val funcName = parts(0)
        val argCount = parts(1).toInt
        val argTypesStr = parts(2).stripPrefix("(").stripSuffix(")")
        val group = parts(3) // 新增 group 字段
        val argTypes: Seq[String] = argTypesStr.split(";").map(_.trim).filter(_.nonEmpty)
        new FunctionMeta(funcName, argCount, argTypes, group)
      }
    } finally {
      source.close()
    }
  }
}