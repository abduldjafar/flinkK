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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

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
	/**
	 *
	 * @param args
	 * @throws Exception
	 *
	 * Class ini digunakan untuk mencari video yang paling banyak disukai
	 * dan jenis apa yang paling banyak disukai
	 */
	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, Integer, Double>> data = env.readTextFile("USvideos.csv")
				.map(new SplitText())
				.filter(new FilterLength())
				.flatMap(new Extract())
				.flatMap(new ConvertToBiasa())
				.filter(new GetMostLikes())
				.sortPartition(2, Order.DESCENDING)
				.setParallelism(1)
				.flatMap(new FlatMapFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, Double>>() {
					@Override
					public void flatMap(Tuple3<String, Integer, Double> data, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {
						collector.collect(new Tuple3<>(data.f0,data.f1,data.f2));
					}
				});


		data.print();
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
			int views = 0;
			if (!strings[7].equals("views")) {
				try{
					views = Integer.parseInt(strings[7]);
				}catch (NumberFormatException e){
					views = 0;
				}
			}
			return strings.length == 16 && views > 100000;
		}
	}

	private static class GetMostLikes implements FilterFunction<Tuple3<String, Integer, Double>> {
		@Override
		public boolean filter(Tuple3<String, Integer, Double> datas) throws Exception {
			return datas.f2 > 20.00;
		}
	}

	private static class Extract implements FlatMapFunction <String[], Tuple3<StringValue,IntValue,DoubleValue>> {
		// Mutable int field to reuse to reduce GC pressure
		StringValue judul = new StringValue();
		IntValue	views = new IntValue();
		DoubleValue	likes = new DoubleValue();

		// Reuse rating value and result tuple
		Tuple3<StringValue, IntValue, DoubleValue> result = new Tuple3<>(judul, views, likes);

		@Override
		public void flatMap(String[] data, Collector<Tuple3<StringValue,IntValue,DoubleValue>> collector) throws Exception {
			// Ignore CSV header
			if (!data[8].equals("likes")) {
				try{
					likes.setValue((Double.parseDouble(data[8])/Double.parseDouble(data[7]))*100);
				}catch (NumberFormatException e){
					likes.setValue(0.00);
				}
			}
			judul.setValue(data[2]);
			views.setValue(Integer.parseInt(data[7]));
			collector.collect(result);
		}
	}

	private static class ConvertToBiasa implements FlatMapFunction<Tuple3<StringValue,IntValue, DoubleValue>,Tuple3<String, Integer, Double>>{
		@Override
		public void flatMap(Tuple3<StringValue, IntValue, DoubleValue> data, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {
			collector.collect(new Tuple3<String, Integer, Double>(data.f0.toString(),Integer.parseInt(data.f1.toString()),
					Double.parseDouble(data.f2.toString())));
		}
	}
}
