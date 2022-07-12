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

package com.calvin.flink.example;

import com.calvin.flink.example.deserialization.CustomDeserialization;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ResourceBundle;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
@Slf4j
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 执行模式可以通过 execute.runtime-mode 设置来配置。有三种可选的值：
		// 	STREAMING: 经典 DataStream 执行模式（默认)
		// 	BATCH: 在 DataStream API 上进行批量式执行
		// 	AUTOMATIC: 让系统根据数据源的边界性来决定
		// 这可以通过 bin/flink run ... 的命令行参数进行配置，或者在创建/配置 StreamExecutionEnvironment 时写进程序。
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		env.enableCheckpointing(3000);
		env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///ck"));
		// 确认 checkpoints 之间的时间会进行 500 ms
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		// 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */
		ResourceBundle resource = ResourceBundle.getBundle("dev");
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
				.hostname(resource.getString("host"))
				.port(3306)
				.username("root")
				.password("123456")
				.databaseList("test")
				.tableList("test.order_info")
				.deserializer(new CustomDeserialization())
				.startupOptions(StartupOptions.initial())
				.build();

		DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

		streamSource.print();

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
