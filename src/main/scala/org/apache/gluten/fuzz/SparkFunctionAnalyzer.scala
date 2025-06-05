package org.apache.gluten.fuzz
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import java.io.{File, PrintWriter}

object SparkFunctionAnalyzer {

  def main(args: Array[String]): Unit = {
    // 解析命令行参数
    val detail = args.contains("--detail")
    val outputFile = "func_args"

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Function Analyzer")
      .master("local[*]")
      .getOrCreate()

    try {
      // 获取内置函数列表
      val functions = getBuiltinFunctions(spark)

      // 创建输出文件
      val writer = new PrintWriter(new File(outputFile))

      try {
        writer.println(s"Spark SQL 内置函数总数: ${functions.size}")
        writer.println("=" * 50)

        // 分析并打印函数信息
        functions.sortBy(_._1).foreach { case (name, clazz) =>
          analyzeFunctionParameters(name, clazz, writer, detail)
        }
      } finally {
        writer.close()
      }

      println(s"分析结果已输出到文件: $outputFile")
    } finally {
      spark.stop()
    }
  }

  /**
   * 获取Spark SQL内置函数列表
   */
  def getBuiltinFunctions(spark: SparkSession): Seq[(String, Class[_])] = {
    val functionRegistry = FunctionRegistry
    // 获取函数映射
    val expressions = functionRegistry.expressions

    // 提取函数名称和类信息
    expressions.toSeq.map { case (funcName, (exprInfo, _)) =>
      val className = exprInfo.getClassName
      try {
        (funcName, Class.forName(className))
      } catch {
        case _: ClassNotFoundException =>
          (funcName, null)
      }
    }.filter(_._2 != null)
  }

  /**
   * 分析函数参数
   */

  def analyzeFunctionParameters(name: String, clazz: Class[_], writer: PrintWriter, detail: Boolean): Unit = {
    if (detail) {
      writer.println(s"实现类: ${clazz.getName}")
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
      writer.println(s"""Func: $name, varArgs""")
    } else {
      // 获取所有构造函数的参数个数
      val validParametersCount = effectiveConstructors
        .map(_.getParameterCount).distinct.sorted

      effectiveConstructors.foreach { ctor =>
        val paramCount = ctor.getParameterCount
        val paramTypes = ctor.getParameterTypes.map(_.getSimpleName).mkString(", ")
        writer.println(s"""Func: $name, $paramCount, ($paramTypes)""")
      }

      if (effectiveConstructors.isEmpty) {
        throw new RuntimeException(s"函数 $name 没有有效的构造函数")
      }
    }

    // 检查是否是RuntimeReplaceable
    if (classOf[RuntimeReplaceable].isAssignableFrom(clazz) && detail) {
      writer.println("注意: 这是一个RuntimeReplaceable函数")
    }
  }
}