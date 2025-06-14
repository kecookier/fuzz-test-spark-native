package org.apache.gluten.fuzz
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class GenerateFunctionArgTypes {

}

// 生成一个表， 每个字段是一个类型， 生成sql时，遍历一个类型
class FunctionMeta(val name: String,
                   val argCount: Int,
                   val argTypes: Seq[String],
                   val group: String // 新增 group 字段
                  ) {}
object GenerateFunctionArgTypes {
  val unaryArithmeticOps: Seq[String] = Seq("+", "-")
  val binaryArithmeticOps: Seq[String] = Seq("+", "-", "*", "/", "%", "&", "|", "^")
  val comparisonOps: Seq[String] = Seq("=", "==", "<=>", ">", ">=", "<", "<=")
  // from_csv 包含Struct类型的参数
  // in 包含Seq Seq暂时不处理
  val blacklist = unaryArithmeticOps ++ binaryArithmeticOps ++ comparisonOps ++ Seq("from_csv", "in", "when", "like")

  val simpleTypeValues: Seq[(DataType, Any)] = Seq(
    (DataTypes.BooleanType, true),
    (DataTypes.ByteType, 1.toByte),
    (DataTypes.ShortType, 1.toShort),
    (DataTypes.IntegerType, 1),
    (DataTypes.LongType, 1L),
    (DataTypes.FloatType, 1.1f),
    (DataTypes.DoubleType, 1.2),
    (DataTypes.createDecimalType(10, 2), java.math.BigDecimal.valueOf(10.2)),
    (DataTypes.DateType, java.sql.Date.valueOf("2000-01-01")),
    (DataTypes.TimestampType, java.sql.Timestamp.valueOf("2000-11-11 00:00:00")),
    (DataTypes.StringType, "a"),
    (DataTypes.BinaryType, Array.fill(10)(1.toByte))
  )

  val complexTypeValues: Seq[(DataType, Any)] = {
    // ArrayType: 每种simpleType都生成一个Array
    val arrayTypes = simpleTypeValues.map { case (dt, v) =>
      (ArrayType(dt), Seq(v, v))
    }
    val mapTypes = for {
      (keyType, keyVal) <- simpleTypeValues
      (valueType, valueVal) <- simpleTypeValues
    } yield {
      (MapType(keyType, valueType), Map(keyVal -> valueVal))
    }

    val structTypes = simpleTypeValues.map { case (dt, v) =>
      (StructType(Seq(StructField("f1", dt))), Row(v))
    }

    arrayTypes ++ mapTypes ++ structTypes
  }

//  val typeValues = simpleTypeValues ++ complexTypeValues
  val typeValues = simpleTypeValues

  // TODO(zhaokuo03) ignore StructType，当前之后 from_csv 函数有这个参数
  def string2DataType(s: String): DataType = {
    s.toLowerCase match {
      case "boolean"    => DataTypes.BooleanType
      case "byte"       => DataTypes.ByteType
      case "short"      => DataTypes.ShortType
      case "int" | "integer" => DataTypes.IntegerType
      case "long"       => DataTypes.LongType
      case "float"      => DataTypes.FloatType
      case "double"     => DataTypes.DoubleType
      case "decimal"    => DataTypes.createDecimalType(10, 2)
      case "date"       => DataTypes.DateType
      case "timestamp"  => DataTypes.TimestampType
      case "string"     => DataTypes.StringType
      case "binary"     => DataTypes.BinaryType
      case arr if arr.startsWith("array<") && arr.endsWith(">") =>
        val elemType = string2DataType(arr.substring(6, arr.length - 1))
        ArrayType(elemType)
      case map if map.startsWith("map<") && map.endsWith(">") =>
        val kv = map.substring(4, map.length - 1).split(",", 2).map(_.trim)
        if (kv.length == 2)
          MapType(string2DataType(kv(0)), string2DataType(kv(1)))
        else
          throw new IllegalArgumentException(s"非法Map类型: $s")
      case struct if struct.startsWith("struct<") && struct.endsWith(">") =>
        // 这里只支持单字段struct<field:type>
        val body = struct.substring(7, struct.length - 1)
        val Array(field, typ) = body.split(":", 2).map(_.trim)
        StructType(Seq(StructField(field, string2DataType(typ))))
      case "option" => DataTypes.NullType
      case "?" => DataTypes.NullType
      case _ =>
        throw new IllegalArgumentException(s"未知类型: $s")
    }
  }

  def genFuncMetaString(func: FunctionMeta): (String,String) = {
    val funcName = func.name
    val argCount = func.argCount
    val argTypes = func.argTypes
    val group = func.group
    // 如果 argTypes全部是Expression， 生成字符串 s"Function($funcName, $argCount)"
    if (argTypes.forall(_ == "Expression")) {
      (s"""Function("$funcName", $argCount)""", s"$group")
    } else {
      (s"""// FunctionWithSignature("$funcName", $argCount) => FunctionMeta($funcName, $argCount, $argTypes, $group)""", s"$group")
    }
  }

  // 添加一个新方法，将函数元数据字符串写入文件
  def splitFuncMetaByGroup(funcs: Seq[FunctionMeta], filename: String): Unit = {
    import java.io.{BufferedWriter, FileWriter}

    // 写入每个函数的元数据字符串
//    val validFuncs = funcs.filterNot(func =>
//      func.group == "agg_funcs" || func.group == "window_funcs" || func.argCount == -1)
    val validFuncs = funcs
    // metaStrings 按照 group 分组，每个分组写入一个文件
    val groupFilename = filename
    val groupWriter = new BufferedWriter(new FileWriter(groupFilename))

    try {
      val metaStrings = validFuncs.map(genFuncMetaString)
      val groupedMetaStrings = metaStrings.groupBy(_._2)
      groupedMetaStrings.foreach {
        case (group, metaStrs) =>
          // 写入文件头部
          groupWriter.write(s"// 自动生成的函数元数据 - $group\n")
          groupWriter.write(s"val ${group}_Funcs = Seq(\n")

          // 写入每一行，除了最后一行外都加逗号
          metaStrs.zipWithIndex.foreach { case ((metaStr, _), idx) =>
            if (idx < metaStrs.length - 1) {
              groupWriter.write(s"  $metaStr,\n")
            } else {
              groupWriter.write(s"  $metaStr\n")
            }
          }
          // 写入文件尾部
          groupWriter.write(")\n\n")
          println(s"成功写入 ${metaStrs.length} 个函数元数据到文件: $groupFilename")
      }
    }
    finally
    {
      groupWriter.close()
    }
  }

  def sqlGen(func: FunctionMeta): Seq[(String, Seq[String])] = {
    val funcName = func.name
    val argTypes = func.argTypes

    // TODO(zhaokuo03) 过滤一些corner case，后续单独处理
    if (blacklist.contains(funcName)
      || (func.group == "agg_funcs" || func.group == "window_funcs")
      || func.argCount == -1) {
      println(s"filter func: $funcName")
      return Seq.empty
    }

    // 获取每个参数的候选值列表
    val argValues: Seq[Seq[Any]] = argTypes.map {
      case "Expression" | "DataType" =>
        typeValues.map(_._2)
      case s =>
        typeValues.filter(_._1 == string2DataType(s)).map(_._2)
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

      val typeStrs = args.map {
        case null => "null"
        case v => v.getClass.getSimpleName
      }

      (s"select $funcName($argStr)", typeStrs)
    }
  }

  def validAllFunctions(funcs: Seq[FunctionMeta]): Unit = {
    if (funcs.isEmpty) {
      return
    }
    for (func <- funcs) {
      println(s"show function: ${func.name}")
    }

    for (func <- funcs) {
      println(s"validate function: ${func.name}")
      val sqls = sqlGen(func)
      println(s"${sqls.length} sqls generated")
      for ((sql, types) <- sqls) {

//        try {
//          // 用 explain 校验 SQL 是否能正确解析和生成执行计划
//          spark.sql("explain " + sql)
//          // 如果需要，也可以继续执行 df.show() 等操作
//        } catch {
//          case e: Exception =>
//            // 记录或处理无法解析/执行的 SQL
//            println(s"SQL 校验失败: $sql, 错误: ${e.getMessage}")
//        }

        // 记录生成的 SQL 和参数类型
        println(s"validate SQL: $sql, 参数类型: ${types.mkString(", ")}")

      }
    }

  }


}
