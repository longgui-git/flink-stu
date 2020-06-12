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

package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TumbleBase;
import org.apache.flink.types.Row;

import java.util.Properties;


public class KafkaStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		org.apache.flink.table.api.java.StreamTableEnvironment tenv = StreamTableEnvironment.getTableEnvironment(env);

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "13.114.15.1:9092,13.114.15.2:9092,13.114.15.6:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("SignalControllerStatus_perception_out_test_", new SimpleStringSchema(), properties);
		consumer.setStartFromLatest();

		DataStreamSource<String> topic = env.addSource(consumer);

		topic.print();

//		SingleOutputStreamOperator<String> map = topic.map(new MapFunction<String, String>() {
//			@Override
//			public String map(String s) throws Exception {
//
//				return s;
//			}
//		});

//		tenv.registerDataStream("books",topic,"name");
//		Table table = tenv.sqlQuery("select name ,count(1) from books group by name");
//
//		DataStream<Tuple2<Boolean, Row>> result = tenv.toRetractStream(table, Row.class);
//		result.print();

		// execute program
		env.execute("kafka test");
	}
}
