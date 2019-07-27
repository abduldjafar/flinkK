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
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	/**
	 * StreaminJob merupakan class yang digunakan untuk preprocessing data
	 * data yang dimaksus adalah data dari twitter yang di ambil dari apache kafka
	 * untuk lebih jelasnya tentang apache kafka bisa dilihat di link dibawah ini
	 * https://kafka.apache.org
	 * preprocessing yang dilakukan adalah sebagai berikut
	 * 1. Data masuk
	 * 2. cleansing
	 * 3. filter stopword
	 * 4. simpan lagi ke kafka
	 */
	private static List<String> stopword;

	private static List<String> cleanTweet(String input){
		/**
		 * fungsi untuk membersihkan tweeter dengan menggunakan beberapa regex
		 */

		input = input.replaceAll("\\&\\w*;","");
		input = input.replaceAll("@[^\\s]+","");
		input = input.replaceAll("rt","");
		input = input.replaceAll("\\$\\w*","");
		input = input.replaceAll("https?:\\/\\/.*\\/\\w*","");
		input = input.replaceAll("#\\w*","");
		input = input.trim().toLowerCase();

		Pattern pattern = Pattern.compile("\\w+");
		List<String> list = new ArrayList<String>();
		Matcher m = pattern.matcher(input);
		while (m.find()) {
			list.add(m.group());
		}


		return list;
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		setStopword("stopword.txt");
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		DataStreamSink<String> stream = env
				.addSource(new FlinkKafkaConsumer<>("kotekaman-tweet", new SimpleStringSchema(), properties))
				.rebalance()
				.map(new CleanFunc())
				.flatMap(new GetWord())
				.addSink(new FlinkKafkaProducer<String>("tweet", new SimpleStringSchema(), properties));

		// execute program
		System.out.println(stopword);
		env.execute("Flink Streaming Java API Skeleton");
	}

	private static class CleanFunc implements MapFunction<String, List<String>> {
		/**
		 *
		 * @param tweet String
		 * @return hasil as List  String
		 * @throws Exception
		 *
		 * Fungsi maping flink yang menerapkan proses cleansing tweet
		 */
		@Override
		public List<String> map(String tweet) throws Exception {
			List<String> hasil = new ArrayList<>();
			String[] temp = cleanTweet(tweet).toArray(new String[0]);
			for (String kata: temp){
				if (stopword.contains(kata)){
					System.out.println("===stopword===");
				}else {
					hasil.add(kata);
				}
			}
			return hasil;
		}
	}


	public static void setStopword(String file) {
		/**
		 * Fungsi untuk mengambil stopword dari text file
		 * dan menyimpannya ke dalam List
		 */
		List<String> myList = new ArrayList<>();

		try (Stream<String> stream = Files.lines(Paths.get(file))) {

			stream.forEach(myList::add);

		} catch (IOException e) {
			e.printStackTrace();
		}
		StreamingJob.stopword = myList;
	}


	private static class GetWord implements FlatMapFunction<List<String>,String> {
		/**
		 *
		 * @param temp String
		 * @param collector Collector
		 * @throws Exception
		 *
		 * penerapan fungsi FlatMap apache flink untuk mengambil data yang ada
		 * di List<string>
		 */
		@Override
		public void flatMap(List<String> temp, Collector<String> collector) throws Exception {
			temp.forEach(collector::collect);
		}
	}

}

