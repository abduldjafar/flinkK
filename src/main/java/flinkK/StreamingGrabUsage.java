package flinkK;

import flinkSink.JdbcSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.sql.DriverManager;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class StreamingGrabUsage {
    public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    String strQuery = "INSERT INTO grab (harga,tanggal) VALUES (?, ?);";

    JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
              .setDrivername("org.postgresql.Driver")
              .setDBUrl("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=toor")
              .setQuery(strQuery)
              .setSqlTypes(new int[] { Types.INTEGER, Types.DATE}) //set the types
              .finish();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    DataStreamSink<Row> stream = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties))
            .rebalance()
            .map(new CleanFunc())
            .addSink(new JdbcSink());

    env.execute("runss");
    }

    private static class CleanFunc implements MapFunction<String, Row> {

        @Override
        public Row map(String data) throws Exception {

            Tuple2<Integer,Date> datas =  new Tuple2<Integer, Date>();
            Row datas2 = new Row(2);

            String[] splits = data.split("\\|");
            String[] tempdata = splits[1].split("Detail")[0].split("WAKTU")[1].split("\\+0700");
            String[] data1= splits[0].split("!")[1].split("RP")[1].split(" ");

            DateFormat formatter;
            Date date;
            formatter = new SimpleDateFormat("dd MMM yy HH:mm");

            date = formatter.parse(tempdata[0]);
            datas.f0 = Integer.valueOf(data1[1]);
            datas.f1 = date;
            datas2.setField(0,datas.f0);
            datas2.setField(1, new java.sql.Date(datas.f1.getTime()));
            System.out.println(datas2.getField(1));

            return datas2;
        }
    }

}

