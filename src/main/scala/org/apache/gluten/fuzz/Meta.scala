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

  // 自动生成的函数元数据 - url_funcs
  val url_funcs_Funcs = Seq(
    // FunctionWithSignature("parse_url", -1) => FunctionMeta(parse_url, -1, WrappedArray(Seq), url_funcs)
  )

  // 自动生成的函数元数据 - conditional_funcs
  val conditional_funcs_Funcs = Seq(
    // FunctionWithSignature("coalesce", -1) => FunctionMeta(coalesce, -1, WrappedArray(Seq), conditional_funcs),
    Function("if", 3),
    Function("ifnull", 2),
    Function("nanvl", 2),
    Function("nullif", 2),
    Function("nvl", 2),
    Function("nvl2", 3),
    // FunctionWithSignature("when", 2) => FunctionMeta(when, 2, WrappedArray(Seq, Option), conditional_funcs)
  )

  // 自动生成的函数元数据 - xml_funcs
  val xml_funcs_Funcs = Seq(
    Function("xpath", 2),
    Function("xpath_boolean", 2),
    Function("xpath_double", 2),
    Function("xpath_float", 2),
    Function("xpath_int", 2),
    Function("xpath_long", 2),
    Function("xpath_number", 2),
    Function("xpath_short", 2),
    Function("xpath_string", 2)
  )

  // 自动生成的函数元数据 - empty
  val empty_Funcs = Seq(
    // FunctionWithSignature("cube", -1) => FunctionMeta(cube, -1, WrappedArray(Seq), empty),
    // FunctionWithSignature("rollup", -1) => FunctionMeta(rollup, -1, WrappedArray(Seq), empty)
  )

  // 自动生成的函数元数据 - array_funcs
  val array_funcs_Funcs = Seq(
    // FunctionWithSignature("array", -1) => FunctionMeta(array, -1, WrappedArray(Seq), array_funcs),
    Function("array_contains", 2),
    Function("array_distinct", 1),
    Function("array_except", 2),
    Function("array_intersect", 2),
    // FunctionWithSignature("array_join", 3) => FunctionMeta(array_join, 3, WrappedArray(Expression, Expression, Option), array_funcs),
    Function("array_join", 3),
    Function("array_join", 2),
    Function("array_max", 1),
    Function("array_min", 1),
    Function("array_position", 2),
    Function("array_remove", 2),
    Function("array_repeat", 2),
    Function("array_union", 2),
    Function("arrays_overlap", 2),
    // FunctionWithSignature("arrays_zip", -1) => FunctionMeta(arrays_zip, -1, WrappedArray(Seq), array_funcs),
    Function("flatten", 1),
    Function("sequence", 3),
    Function("sequence", 2),
    // FunctionWithSignature("sequence", 4) => FunctionMeta(sequence, 4, WrappedArray(Expression, Expression, Option, Option), array_funcs),
    // FunctionWithSignature("shuffle", 2) => FunctionMeta(shuffle, 2, WrappedArray(Expression, Option), array_funcs),
    Function("shuffle", 1),
    Function("slice", 3),
    Function("sort_array", 2),
    Function("sort_array", 1)
  )

  // 自动生成的函数元数据 - datetime_funcs
  val datetime_funcs_Funcs = Seq(
    // FunctionWithSignature("add_months", 3) => FunctionMeta(add_months, 3, WrappedArray(Expression, Expression, boolean), datetime_funcs),
    Function("add_months", 2),
    // FunctionWithSignature("current_date", 1) => FunctionMeta(current_date, 1, WrappedArray(Option), datetime_funcs),
    Function("current_date", 0),
    Function("current_timestamp", 0),
    Function("date_add", 2),
    // FunctionWithSignature("date_format", 3) => FunctionMeta(date_format, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("date_format", 2),
    Function("date_part", 2),
    Function("date_sub", 2),
    // FunctionWithSignature("date_trunc", 3) => FunctionMeta(date_trunc, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("date_trunc", 2),
    Function("datediff", 2),
    Function("day", 1),
    Function("dayofmonth", 1),
    Function("dayofweek", 1),
    Function("dayofyear", 1),
    Function("extract", 2),
    // FunctionWithSignature("from_unixtime", 3) => FunctionMeta(from_unixtime, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("from_unixtime", 2),
    Function("from_unixtime", 1),
    Function("from_utc_timestamp", 2),
    Function("hour", 1),
    // FunctionWithSignature("hour", 2) => FunctionMeta(hour, 2, WrappedArray(Expression, Option), datetime_funcs),
    Function("last_day", 1),
    Function("make_date", 3),
    Function("make_interval", 4),
    Function("make_interval", 3),
    Function("make_interval", 2),
    Function("make_interval", 1),
    Function("make_interval", 0),
    Function("make_interval", 7),
    Function("make_interval", 6),
    Function("make_interval", 5),
    Function("make_timestamp", 6),
    // FunctionWithSignature("make_timestamp", 8) => FunctionMeta(make_timestamp, 8, WrappedArray(Expression, Expression, Expression, Expression, Expression, Expression, Option, Option), datetime_funcs),
    Function("make_timestamp", 7),
    Function("minute", 1),
    // FunctionWithSignature("minute", 2) => FunctionMeta(minute, 2, WrappedArray(Expression, Option), datetime_funcs),
    Function("month", 1),
    // FunctionWithSignature("months_between", 4) => FunctionMeta(months_between, 4, WrappedArray(Expression, Expression, Expression, Option), datetime_funcs),
    Function("months_between", 3),
    Function("months_between", 2),
    Function("next_day", 2),
    Function("now", 0),
    Function("quarter", 1),
    Function("second", 1),
    // FunctionWithSignature("second", 2) => FunctionMeta(second, 2, WrappedArray(Expression, Option), datetime_funcs),
    Function("to_date", 1),
    Function("to_timestamp", 2),
    Function("to_timestamp", 1),
    Function("to_unix_timestamp", 2),
    // FunctionWithSignature("to_unix_timestamp", 3) => FunctionMeta(to_unix_timestamp, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("to_unix_timestamp", 1),
    Function("to_utc_timestamp", 2),
    Function("trunc", 2),
    Function("unix_timestamp", 0),
    // FunctionWithSignature("unix_timestamp", 3) => FunctionMeta(unix_timestamp, 3, WrappedArray(Expression, Expression, Option), datetime_funcs),
    Function("unix_timestamp", 2),
    Function("unix_timestamp", 1),
    Function("weekday", 1),
    Function("weekofyear", 1),
    Function("window", 4),
    // FunctionWithSignature("window", 4) => FunctionMeta(window, 4, WrappedArray(Expression, long, long, long), datetime_funcs),
    Function("window", 3),
    Function("window", 2),
    Function("year", 1)
  )

  // 自动生成的函数元数据 - window_funcs
  val window_funcs_Funcs = Seq(
    Function("cume_dist", 0),
    // FunctionWithSignature("dense_rank", -1) => FunctionMeta(dense_rank, -1, WrappedArray(Seq), window_funcs),
    Function("lag", 0),
    Function("lag", 3),
    Function("lag", 2),
    Function("lag", 1),
    Function("lead", 0),
    Function("lead", 3),
    Function("lead", 2),
    Function("lead", 1),
    Function("ntile", 0),
    Function("ntile", 1),
    // FunctionWithSignature("percent_rank", -1) => FunctionMeta(percent_rank, -1, WrappedArray(Seq), window_funcs),
    // FunctionWithSignature("rank", -1) => FunctionMeta(rank, -1, WrappedArray(Seq), window_funcs),
    Function("row_number", 0)
  )

  // 自动生成的函数元数据 - string_funcs
  val string_funcs_Funcs = Seq(
    Function("ascii", 1),
    Function("base64", 1),
    Function("bit_length", 1),
    Function("char", 1),
    Function("char_length", 1),
    Function("character_length", 1),
    Function("chr", 1),
    // FunctionWithSignature("concat_ws", -1) => FunctionMeta(concat_ws, -1, WrappedArray(Seq), string_funcs),
    Function("decode", 2),
    // FunctionWithSignature("elt", -1) => FunctionMeta(elt, -1, WrappedArray(Seq), string_funcs),
    Function("encode", 2),
    Function("find_in_set", 2),
    Function("format_number", 2),
    // FunctionWithSignature("format_string", -1) => FunctionMeta(format_string, -1, WrappedArray(Seq), string_funcs),
    Function("initcap", 1),
    Function("instr", 2),
    Function("lcase", 1),
    Function("left", 2),
    Function("length", 1),
    Function("levenshtein", 2),
    Function("locate", 2),
    Function("locate", 3),
    Function("lower", 1),
    Function("lpad", 2),
    Function("lpad", 3),
    Function("ltrim", 2),
    Function("ltrim", 1),
    // FunctionWithSignature("ltrim", 2) => FunctionMeta(ltrim, 2, WrappedArray(Expression, Option), string_funcs),
    Function("octet_length", 1),
    Function("overlay", 4),
    Function("overlay", 3),
    Function("position", 2),
    Function("position", 3),
    // FunctionWithSignature("printf", -1) => FunctionMeta(printf, -1, WrappedArray(Seq), string_funcs),
    Function("regexp_extract", 2),
    Function("regexp_extract", 3),
    Function("regexp_replace", 3),
    Function("repeat", 2),
    Function("replace", 3),
    Function("replace", 2),
    Function("right", 2),
    Function("rpad", 2),
    Function("rpad", 3),
    Function("rtrim", 2),
    Function("rtrim", 1),
    // FunctionWithSignature("rtrim", 2) => FunctionMeta(rtrim, 2, WrappedArray(Expression, Option), string_funcs),
    Function("sentences", 3),
    Function("sentences", 1),
    Function("sentences", 2),
    Function("soundex", 1),
    Function("space", 1),
    Function("split", 3),
    Function("split", 2),
    Function("substr", 2),
    Function("substr", 3),
    Function("substring", 2),
    Function("substring", 3),
    Function("substring_index", 3),
    Function("translate", 3),
    Function("trim", 2),
    Function("trim", 1),
    // FunctionWithSignature("trim", 2) => FunctionMeta(trim, 2, WrappedArray(Expression, Option), string_funcs),
    Function("ucase", 1),
    Function("unbase64", 1),
    Function("upper", 1)
  )

  // 自动生成的函数元数据 - csv_funcs
  val csv_funcs_Funcs = Seq(
    // FunctionWithSignature("from_csv", 3) => FunctionMeta(from_csv, 3, WrappedArray(Expression, Expression, Map<String, String>), csv_funcs),
    Function("from_csv", 3),
    Function("from_csv", 2),
    // FunctionWithSignature("from_csv", 4) => FunctionMeta(from_csv, 4, WrappedArray(Struct<?>, Map<String, String>, Expression, Option), csv_funcs),
    // FunctionWithSignature("schema_of_csv", 2) => FunctionMeta(schema_of_csv, 2, WrappedArray(Expression, Map<String, String>), csv_funcs),
    Function("schema_of_csv", 2),
    Function("schema_of_csv", 1),
    Function("to_csv", 2),
    // FunctionWithSignature("to_csv", 2) => FunctionMeta(to_csv, 2, WrappedArray(Map<String, String>, Expression), csv_funcs),
    Function("to_csv", 1),
    // FunctionWithSignature("to_csv", 3) => FunctionMeta(to_csv, 3, WrappedArray(Map<String, String>, Expression, Option), csv_funcs)
  )

  // 自动生成的函数元数据 - misc_funcs
  val misc_funcs_Funcs = Seq(
    Function("assert_true", 1),
    Function("current_database", 0),
    Function("input_file_block_length", 0),
    Function("input_file_block_start", 0),
    Function("input_file_name", 0),
    // FunctionWithSignature("java_method", -1) => FunctionMeta(java_method, -1, WrappedArray(Seq), misc_funcs),
    Function("monotonically_increasing_id", 0),
    // FunctionWithSignature("reflect", -1) => FunctionMeta(reflect, -1, WrappedArray(Seq), misc_funcs),
    Function("spark_partition_id", 0),
    Function("typeof", 1),
    // FunctionWithSignature("uuid", 1) => FunctionMeta(uuid, 1, WrappedArray(Option), misc_funcs),
    Function("uuid", 0),
    Function("version", 0)
  )

  // 自动生成的函数元数据 - lambda_funcs
  val lambda_funcs_Funcs = Seq(
    Function("aggregate", 4),
    Function("aggregate", 3),
    Function("array_sort", 2),
    Function("array_sort", 1),
    Function("exists", 2),
    // FunctionWithSignature("exists", 3) => FunctionMeta(exists, 3, WrappedArray(Expression, Expression, boolean), lambda_funcs),
    Function("filter", 2),
    Function("forall", 2),
    Function("map_filter", 2),
    Function("map_zip_with", 3),
    Function("transform", 2),
    Function("transform_keys", 2),
    Function("transform_values", 2),
    Function("zip_with", 3)
  )

  // 自动生成的函数元数据 - conversion_funcs
  val conversion_funcs_Funcs = Seq(
    // FunctionWithSignature("bigint", 3) => FunctionMeta(bigint, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("binary", 3) => FunctionMeta(binary, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("boolean", 3) => FunctionMeta(boolean, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("cast", 3) => FunctionMeta(cast, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("date", 3) => FunctionMeta(date, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("decimal", 3) => FunctionMeta(decimal, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("double", 3) => FunctionMeta(double, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("float", 3) => FunctionMeta(float, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("int", 3) => FunctionMeta(int, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("smallint", 3) => FunctionMeta(smallint, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("string", 3) => FunctionMeta(string, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("timestamp", 3) => FunctionMeta(timestamp, 3, WrappedArray(Expression, DataType, Option), conversion_funcs),
    // FunctionWithSignature("tinyint", 3) => FunctionMeta(tinyint, 3, WrappedArray(Expression, DataType, Option), conversion_funcs)
  )

  // 自动生成的函数元数据 - bitwise_funcs
  val bitwise_funcs_Funcs = Seq(
    Function("&", 2),
    Function("^", 2),
    Function("bit_count", 1),
    Function("shiftright", 2),
    Function("shiftrightunsigned", 2),
    Function("|", 2),
    Function("~", 1)
  )

  // 自动生成的函数元数据 - agg_funcs
  val agg_funcs_Funcs = Seq(
    Function("any", 1),
    Function("approx_count_distinct", 1),
    // FunctionWithSignature("approx_count_distinct", 4) => FunctionMeta(approx_count_distinct, 4, WrappedArray(Expression, double, int, int), agg_funcs),
    Function("approx_count_distinct", 2),
    // FunctionWithSignature("approx_percentile", 5) => FunctionMeta(approx_percentile, 5, WrappedArray(Expression, Expression, Expression, int, int), agg_funcs),
    Function("approx_percentile", 2),
    Function("approx_percentile", 3),
    Function("avg", 1),
    Function("bit_and", 1),
    Function("bit_or", 1),
    Function("bit_xor", 1),
    Function("bool_and", 1),
    Function("bool_or", 1),
    // FunctionWithSignature("collect_list", 3) => FunctionMeta(collect_list, 3, WrappedArray(Expression, int, int), agg_funcs),
    Function("collect_list", 1),
    Function("collect_set", 1),
    // FunctionWithSignature("collect_set", 3) => FunctionMeta(collect_set, 3, WrappedArray(Expression, int, int), agg_funcs),
    Function("corr", 2),
    // FunctionWithSignature("count", -1) => FunctionMeta(count, -1, WrappedArray(Seq), agg_funcs),
    Function("count_if", 1),
    // FunctionWithSignature("count_min_sketch", 6) => FunctionMeta(count_min_sketch, 6, WrappedArray(Expression, Expression, Expression, Expression, int, int), agg_funcs),
    Function("count_min_sketch", 4),
    Function("covar_pop", 2),
    Function("covar_samp", 2),
    Function("every", 1),
    // FunctionWithSignature("first", 2) => FunctionMeta(first, 2, WrappedArray(Expression, boolean), agg_funcs),
    Function("first", 1),
    Function("first", 2),
    // FunctionWithSignature("first_value", 2) => FunctionMeta(first_value, 2, WrappedArray(Expression, boolean), agg_funcs),
    Function("first_value", 1),
    Function("first_value", 2),
    Function("grouping", 1),
    // FunctionWithSignature("grouping_id", -1) => FunctionMeta(grouping_id, -1, WrappedArray(Seq), agg_funcs),
    Function("kurtosis", 1),
    // FunctionWithSignature("last", 2) => FunctionMeta(last, 2, WrappedArray(Expression, boolean), agg_funcs),
    Function("last", 1),
    Function("last", 2),
    // FunctionWithSignature("last_value", 2) => FunctionMeta(last_value, 2, WrappedArray(Expression, boolean), agg_funcs),
    Function("last_value", 1),
    Function("last_value", 2),
    Function("max", 1),
    Function("max_by", 2),
    Function("mean", 1),
    Function("min", 1),
    Function("min_by", 2),
    // FunctionWithSignature("percentile", 5) => FunctionMeta(percentile, 5, WrappedArray(Expression, Expression, Expression, int, int), agg_funcs),
    Function("percentile", 2),
    Function("percentile", 3),
    // FunctionWithSignature("percentile_approx", 5) => FunctionMeta(percentile_approx, 5, WrappedArray(Expression, Expression, Expression, int, int), agg_funcs),
    Function("percentile_approx", 2),
    Function("percentile_approx", 3),
    Function("skewness", 1),
    Function("some", 1),
    Function("std", 1),
    Function("stddev", 1),
    Function("stddev_pop", 1),
    Function("stddev_samp", 1),
    Function("sum", 1),
    Function("var_pop", 1),
    Function("var_samp", 1),
    Function("variance", 1)
  )

  // 自动生成的函数元数据 - math_funcs
  val math_funcs_Funcs = Seq(
    Function("%", 2),
    Function("*", 2),
    Function("+", 2),
    Function("-", 2),
    Function("/", 2),
    Function("abs", 1),
    Function("acos", 1),
    Function("acosh", 1),
    Function("asin", 1),
    Function("asinh", 1),
    Function("atan", 1),
    Function("atan2", 2),
    Function("atanh", 1),
    Function("bin", 1),
    Function("bround", 2),
    Function("bround", 1),
    Function("cbrt", 1),
    Function("ceil", 1),
    Function("ceiling", 1),
    Function("conv", 3),
    Function("cos", 1),
    Function("cosh", 1),
    Function("cot", 1),
    Function("degrees", 1),
    Function("div", 2),
    Function("e", 0),
    Function("exp", 1),
    Function("exp_legacy", 1),
    Function("expm1", 1),
    Function("expm1_legacy", 1),
    Function("factorial", 1),
    Function("floor", 1),
    // FunctionWithSignature("greatest", -1) => FunctionMeta(greatest, -1, WrappedArray(Seq), math_funcs),
    Function("hex", 1),
    Function("hypot", 2),
    // FunctionWithSignature("least", -1) => FunctionMeta(least, -1, WrappedArray(Seq), math_funcs),
    Function("ln", 1),
    Function("ln_legacy", 1),
    Function("log", 2),
    Function("log", 1),
    Function("log10", 1),
    Function("log10_legacy", 1),
    Function("log1p", 1),
    Function("log1p_legacy", 1),
    Function("log2", 1),
    Function("log2_legacy", 1),
    Function("log_legacy", 2),
    Function("log_legacy", 1),
    Function("mod", 2),
    Function("negative", 1),
    Function("pi", 0),
    Function("pmod", 2),
    Function("positive", 1),
    Function("pow", 2),
    Function("pow_legacy", 2),
    Function("power", 2),
    Function("power_legacy", 2),
    Function("radians", 1),
    Function("rand", 0),
    Function("rand", 1),
    Function("randn", 0),
    Function("randn", 1),
    Function("random", 0),
    Function("random", 1),
    Function("rint", 1),
    Function("round", 2),
    Function("round", 1),
    Function("shiftleft", 2),
    Function("sign", 1),
    Function("signum", 1),
    Function("sin", 1),
    Function("sinh", 1),
    Function("sqrt", 1),
    Function("tan", 1),
    Function("tanh", 1),
    Function("unhex", 1)
  )

  // 自动生成的函数元数据 - hash_funcs
  val hash_funcs_Funcs = Seq(
    Function("crc32", 1),
    // FunctionWithSignature("hash", -1) => FunctionMeta(hash, -1, WrappedArray(Seq), hash_funcs),
    Function("md5", 1),
    Function("sha", 1),
    Function("sha1", 1),
    Function("sha2", 2),
    // FunctionWithSignature("xxhash64", -1) => FunctionMeta(xxhash64, -1, WrappedArray(Seq), hash_funcs)
  )

  // 自动生成的函数元数据 - struct_funcs
  val struct_funcs_Funcs = Seq(
    // FunctionWithSignature("named_struct", -1) => FunctionMeta(named_struct, -1, WrappedArray(Seq), struct_funcs),
    // FunctionWithSignature("struct", -1) => FunctionMeta(struct, -1, WrappedArray(Seq), struct_funcs)
  )

  // 自动生成的函数元数据 - generator_funcs
  val generator_funcs_Funcs = Seq(
    Function("explode", 1),
    Function("explode_outer", 1),
    Function("inline", 1),
    Function("inline_outer", 1),
    Function("posexplode", 1),
    Function("posexplode_outer", 1),
    // FunctionWithSignature("stack", -1) => FunctionMeta(stack, -1, WrappedArray(Seq), generator_funcs)
  )

  // 自动生成的函数元数据 - predicate_funcs
  val predicate_funcs_Funcs = Seq(
    Function("!", 1),
    Function("<", 2),
    Function("<=", 2),
    Function("<=>", 2),
    Function("=", 2),
    Function("==", 2),
    Function(">", 2),
    Function(">=", 2),
    Function("and", 2),
    // FunctionWithSignature("in", 2) => FunctionMeta(in, 2, WrappedArray(Expression, Seq), predicate_funcs),
    Function("isnan", 1),
    Function("isnotnull", 1),
    Function("isnull", 1),
    // FunctionWithSignature("like", 3) => FunctionMeta(like, 3, WrappedArray(Expression, Expression, char), predicate_funcs),
    Function("like", 2),
    Function("not", 1),
    Function("or", 2),
    Function("regexp", 2),
    Function("rlike", 2)
  )

  // 自动生成的函数元数据 - map_funcs
  val map_funcs_Funcs = Seq(
    Function("element_at", 2),
    // FunctionWithSignature("map", -1) => FunctionMeta(map, -1, WrappedArray(Seq), map_funcs),
    // FunctionWithSignature("map_concat", -1) => FunctionMeta(map_concat, -1, WrappedArray(Seq), map_funcs),
    Function("map_entries", 1),
    Function("map_from_arrays", 2),
    Function("map_from_entries", 1),
    Function("map_keys", 1),
    Function("map_values", 1),
    Function("str_to_map", 1),
    Function("str_to_map", 3),
    Function("str_to_map", 2)
  )

  // 自动生成的函数元数据 - json_funcs
  val json_funcs_Funcs = Seq(
    Function("from_json", 2),
    // FunctionWithSignature("from_json", 3) => FunctionMeta(from_json, 3, WrappedArray(Expression, Expression, Map<String, String>), json_funcs),
    Function("from_json", 3),
    // FunctionWithSignature("from_json", 4) => FunctionMeta(from_json, 4, WrappedArray(DataType, Map<String, String>, Expression, Option), json_funcs),
    // FunctionWithSignature("from_json_legacy", 4) => FunctionMeta(from_json_legacy, 4, WrappedArray(DataType, Map<String, String>, Expression, Option), json_funcs),
    // FunctionWithSignature("from_json_legacy", 3) => FunctionMeta(from_json_legacy, 3, WrappedArray(DataType, Map<String, String>, Expression), json_funcs),
    Function("from_json_legacy", 2),
    Function("from_json_legacy", 3),
    Function("get_json_object", 2),
    // FunctionWithSignature("json_tuple", -1) => FunctionMeta(json_tuple, -1, WrappedArray(Seq), json_funcs),
    Function("schema_of_json", 1),
    // FunctionWithSignature("schema_of_json", 2) => FunctionMeta(schema_of_json, 2, WrappedArray(Expression, Map<String, String>), json_funcs),
    Function("schema_of_json", 2),
    Function("to_json", 1),
    // FunctionWithSignature("to_json", 2) => FunctionMeta(to_json, 2, WrappedArray(Map<String, String>, Expression), json_funcs),
    // FunctionWithSignature("to_json", 3) => FunctionMeta(to_json, 3, WrappedArray(Map<String, String>, Expression, Option), json_funcs),
    Function("to_json", 2)
  )

  // 自动生成的函数元数据 - collection_funcs
  val collection_funcs_Funcs = Seq(
    // FunctionWithSignature("cardinality", 2) => FunctionMeta(cardinality, 2, WrappedArray(Expression, boolean), collection_funcs),
    Function("cardinality", 1),
    // FunctionWithSignature("concat", -1) => FunctionMeta(concat, -1, WrappedArray(Seq), collection_funcs),
    Function("reverse", 1),
    // FunctionWithSignature("size", 2) => FunctionMeta(size, 2, WrappedArray(Expression, boolean), collection_funcs),
    Function("size", 1)
  )

  val scalarFunc: Seq[Function] =
    stringScalarFunc ++
    url_funcs_Funcs ++
    math_funcs_Funcs ++
    hash_funcs_Funcs ++
    struct_funcs_Funcs ++
    generator_funcs_Funcs ++
    predicate_funcs_Funcs ++
    map_funcs_Funcs ++
    json_funcs_Funcs

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
