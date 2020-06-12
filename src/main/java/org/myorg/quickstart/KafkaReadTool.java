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

import adu.st.TrafficObstaclesProto;
import adu.st.TrafficPerceptionProto;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class KafkaReadTool {

	public static void main(String[] args) throws Exception {

		// perception_out_test_1390515961993707490
		String debugCrossId = "1390515961993707490";
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		Properties props = new Properties();

		props.put("bootstrap.servers",
				parameterTool.get("brokers", "127.0.0.1:9092"));
		props.setProperty("group.id", "testwh");
		props.put("enable.auto.commit", "true");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("auto.offset.reset", "latest");
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(props);

		consumer.subscribe(Arrays.asList(parameterTool.get("topic", "perception_out_test_"+debugCrossId)));


		for (int i = 0; i < 100000; i++) {
			ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(10));
			for (ConsumerRecord<String, byte[]> record : records) {
				TrafficPerceptionProto.TrafficPerception.Builder tpBuilder = TrafficPerceptionProto.TrafficPerception.newBuilder();
				tpBuilder.setTrafficObstacles(TrafficObstaclesProto.TrafficObstacles.parseFrom(record.value()));
				TrafficPerceptionProto.TrafficPerception build = tpBuilder.build();
				System.out.println(build.toString());
			}

			Thread.sleep(1000);
		}

	}
}
