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

package flinkK;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJobFilterExamp {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> data = env.readTextFile("USvideos.csv")
				.map(new SplitText())
				.filter(new FilterLength())
				.map(new JoinStr());
		System.out.println(data.count());
	}

	private static class SplitText implements MapFunction<String,String[]> {
		@Override
		public String[] map(String s) throws Exception {
			String[] splits = s.split(",");
			return splits;
		}
	}

	private static class FilterLength implements FilterFunction<String[]> {
		@Override
		public boolean filter(String[] strings) throws Exception {
			return strings.length == 16;
		}
	}

	private static class JoinStr implements MapFunction<String[],String> {
		@Override
		public String map(String[] s) throws Exception {
			return String.join(",",s);
		}
	}
}