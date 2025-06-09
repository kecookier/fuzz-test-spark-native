package org.apache.gluten.fuzz
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class GenerateFunctionArgTypes {

}

class FunctionMeta(val name: String,
                   val argCount: Int,
                   val argTypes: Seq[String],
                   val group: String // 新增 group 字段
                  ) {}
object GenerateFunctionArgTypes {
  val unaryArithmeticOps: Seq[String] = Seq("+", "-")
  val binaryArithmeticOps: Seq[String] = Seq("+", "-", "*", "/", "%", "&", "|", "^")
  val comparisonOps: Seq[String] = Seq("=", "==", "<=>", ">", ">=", "<", "<=")
  val blacklist = unaryArithmeticOps ++ binaryArithmeticOps ++ comparisonOps

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