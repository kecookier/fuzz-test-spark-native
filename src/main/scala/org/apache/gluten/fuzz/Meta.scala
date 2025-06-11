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

import Utils._
import org.apache.spark.sql.types._

object Meta {

  val dataTypes: Seq[(DataType, Double)] = Seq(
    (DataTypes.BooleanType, 0.1),
    (DataTypes.ByteType, 0.2),
    (DataTypes.ShortType, 0.2),
    (DataTypes.IntegerType, 0.2),
    (DataTypes.LongType, 0.2),
    (DataTypes.FloatType, 0.2),
    (DataTypes.DoubleType, 0.2),
    (DataTypes.createDecimalType(10, 2), 0.2),
    (DataTypes.DateType, 0.2),
    (DataTypes.TimestampType, 0.2),
    // TimestampNTZType only in Spark 3.4+
    // (DataTypes.TimestampNTZType, 0.2),
    (DataTypes.StringType, 0.2),
    (DataTypes.BinaryType, 0.1))

  val stringScalarFunc: Seq[Function] = Seq(
    Function("substring", 3),
    Function("substring", 2),
    Function("coalesce", 1),
    Function("starts_with", 2),
    Function("ends_with", 2),
    Function("contains", 2),
    Function("ascii", 1),
    Function("bit_length", 1),
    Function("octet_length", 1),
    Function("upper", 1),
    Function("lower", 1),
    Function("chr", 1),
    Function("init_cap", 1),
    Function("trim", 1),
    Function("ltrim", 1),
    Function("rtrim", 1),
    Function("string_space", 1),
    FunctionWithSignature("rpad", 2, StringType, Seq(StringType, ScalarValueType(StringType, () => randomInt(10).toString))),
    FunctionWithSignature("rpad", 3, StringType, Seq(StringType, ScalarValueType(StringType, () => randomInt(10).toString), StringType)),
    FunctionWithSignature("rpad", 2, BinaryType, Seq(BinaryType, ScalarValueType(StringType, () => randomInt(10).toString))),
    FunctionWithSignature("rpad", 3, BinaryType, Seq(BinaryType, ScalarValueType(StringType, () => randomInt(10).toString), BinaryType)),
    Function("hex", 1),
    Function("unhex", 1),
    Function("xxhash64", 1),
    Function("sha1", 1),
    FunctionWithSignature("sha2", 1, StringType, Seq(ScalarValueType(IntegerType, () => randomChoice(Seq(0, 224, 256, 384, 512)).toString))),
    Function("btrim", 1),
    Function("concat_ws", 2),
    FunctionWithSignature("repeat", 2, StringType, Seq(StringType, ScalarValueType(StringType, () => randomInt(10).toString))),
    Function("length", 1),
    Function("reverse", 1),
    Function("in_str", 2),
    Function("replace", 2),
    Function("translate", 2))

  val dateScalarFunc: Seq[Function] = Seq(
    // TODO(zhaokuo) 确认最后的option参数含义
    Function("add_months", 2),
    //  FunctionWithSignature("add_months", 3) => FunctionMeta(add_months, 3, WrappedArray(Expression, Expression, boolean), datetime_funcs),
    //  FunctionWithSignature("current_date", 1) => FunctionMeta(current_date, 1, WrappedArray(Option), datetime_funcs),
    Function("current_date", 0),
    Function("current_timestamp", 0),
    Function("date_add", 2),
    //  FunctionWithSignature("date_format", 3) => FunctionMeta(date_format, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("date_format", 2),
    Function("date_part", 2),
    Function("date_sub", 2),
    //  FunctionWithSignature("date_trunc", 3) => FunctionMeta(date_trunc, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("date_trunc", 2),
    Function("datediff", 2),
    Function("dayofweek", 1),
    Function("dayofyear", 1),
    //  FunctionWithSignature("from_unixtime", 3) => FunctionMeta(from_unixtime, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("from_unixtime", 2),
    Function("from_unixtime", 1),
    Function("from_utc_timestamp", 2),
    Function("hour", 1),
    //  FunctionWithSignature("hour", 2) => FunctionMeta(hour, 2, WrappedArray(Expression, Option), datetime_funcs),
    Function("last_day", 1),
    Function("make_date", 3),
    Function("make_timestamp", 6),
    //  FunctionWithSignature("make_timestamp", 8) => FunctionMeta(make_timestamp, 8, WrappedArray(Expression, Expression, Expression, Expression, Expression, Expression, Option, Option), datetime_funcs),
    Function("make_timestamp", 7),
    Function("minute", 1),
    //  FunctionWithSignature("minute", 2) => FunctionMeta(minute, 2, WrappedArray(Expression, Option), datetime_funcs),
    Function("month", 1),
    //  FunctionWithSignature("months_between", 4) => FunctionMeta(months_between, 4, WrappedArray(Expression, Expression, Expression, Option), datetime_funcs),
    Function("months_between", 2),
    Function("months_between", 3),
    Function("next_day", 2),
    Function("now", 0),
    Function("quarter", 1),
    Function("second", 1),
    //  FunctionWithSignature("second", 2) => FunctionMeta(second, 2, WrappedArray(Expression, Option), datetime_funcs),
    Function("to_timestamp", 2),
    Function("to_timestamp", 1),
    //  FunctionWithSignature("to_unix_timestamp", 3) => FunctionMeta(to_unix_timestamp, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("to_unix_timestamp", 2),
    Function("to_unix_timestamp", 1),
    Function("to_utc_timestamp", 2),
    Function("trunc", 2),
    Function("unix_timestamp", 0),
    //  FunctionWithSignature("unix_timestamp", 3) => FunctionMeta(unix_timestamp, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("unix_timestamp", 2),
    Function("unix_timestamp", 1),
    Function("weekday", 1),
    Function("weekofyear", 1),
    Function("year", 1)
  )

  val mathScalarFunc: Seq[Function] = Seq(
    Function("abs", 1),
    Function("acos", 1),
    Function("asin", 1),
    Function("atan", 1),
    Function("Atan2", 1),
    Function("Cos", 1),
    Function("Exp", 2),
    Function("Ln", 1),
    Function("Log10", 1),
    Function("Log2", 1),
    Function("Pow", 2),
    Function("Round", 1),
    Function("Signum", 1),
    Function("Sin", 1),
    Function("Sqrt", 1),
    Function("Tan", 1),
    Function("Ceil", 1),
    Function("Floor", 1),
    Function("bool_and", 1),
    Function("bool_or", 1),
    Function("bitwise_not", 1))

  // 自动生成的函数元数据 - json_funcs
  val jsonScalarFunc = Seq(
    // TODO(zhaokuo) 处理带有Map<string, string> 的参数
    Function("from_json", 2),
    //    FunctionWithSignature("from_json", 3) => FunctionMeta(from_json, 3, WrappedArray(Expression, Expression, Map<String, String>), json_funcs),
    Function("from_json", 3),
    //  FunctionWithSignature("from_json", 4) => FunctionMeta(from_json, 4, WrappedArray(DataType, Map<String, String>, Expression, Option), json_funcs),
    Function("get_json_object", 2),
    //  FunctionWithSignature("schema_of_json", 2) => FunctionMeta(schema_of_json, 2, WrappedArray(Expression, Map<String, String>), json_funcs),
    Function("schema_of_json", 1),
    Function("schema_of_json", 2),
    Function("to_json", 1),
    //  FunctionWithSignature("to_json", 2) => FunctionMeta(to_json, 2, WrappedArray(Map<String, String>, Expression), json_funcs),
    Function("to_json", 2)
    //  FunctionWithSignature("to_json", 3) => FunctionMeta(to_json, 3, WrappedArray(Map<String, String>, Expression, Option), json_funcs)
  )

  // TODO(zhaokuo) 参数里需要有array类型，待支持
  val arrayScalarFunc = Seq(
    Function("array_contains", 2),
    Function("array_distinct", 1),
    Function("array_except", 2),
    Function("array_intersect", 2),
    Function("array_join", 3),
    //    FunctionWithSignature("array_join", 3) => FunctionMeta(array_join, 3, WrappedArray(Expression, Expression, Option), array_funcs),
    Function("array_join", 2),
    Function("array_max", 1),
    Function("array_min", 1),
    Function("array_position", 2),
    Function("array_remove", 2),
    Function("array_repeat", 2),
    Function("array_union", 2),
    Function("arrays_overlap", 2),
    Function("flatten", 1),
    Function("reverse", 1),
    Function("sequence", 2),
    //  FunctionWithSignature("sequence", 4) => FunctionMeta(sequence, 4, WrappedArray(Expression, Expression, Option, Option), array_funcs),
    Function("sequence", 3),
    //  FunctionWithSignature("shuffle", 2) => FunctionMeta(shuffle, 2, WrappedArray(Expression, Option), array_funcs),
    Function("shuffle", 1),
    Function("slice", 3),
    Function("sort_array", 2),
    Function("sort_array", 1)
  )

  // TODO(zhaokuo) 参数里需要有map类型，待支持
  val mapScalarFunc = Seq(
    Function("map_entries", 1),
    Function("map_from_entries", 1),
    Function("map_keys", 1),
    Function("map_values", 1)
  )

  val miscScalarFunc: Seq[Function] =
    Seq(Function("isnan", 1), Function("isnull", 1), Function("isnotnull", 1))

  val scalarFunc: Seq[Function] = stringScalarFunc ++ dateScalarFunc ++
    mathScalarFunc ++ miscScalarFunc ++  jsonScalarFunc

  val aggFunc: Seq[Function] = Seq(
    Function("min", 1),
    Function("max", 1),
    Function("count", 1),
    Function("avg", 1),
    Function("sum", 1),
    Function("first", 1),
    Function("last", 1),
    Function("var_pop", 1),
    Function("var_samp", 1),
    Function("covar_pop", 1),
    Function("covar_samp", 1),
    Function("stddev_pop", 1),
    Function("stddev_samp", 1),
    Function("corr", 2))

  val unaryArithmeticOps: Seq[String] = Seq("+", "-")

  val binaryArithmeticOps: Seq[String] = Seq("+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "div")

  val comparisonOps: Seq[String] = Seq("=", "<=>", ">", ">=", "<", "<=")

}
