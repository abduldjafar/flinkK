package flinkK;

import flinkSink.JdbcSinkGrabDrive;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class StreamingGrabUsage {
    public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    DataStreamSink<Row> stream = env.addSource(new FlinkKafkaConsumer<>("emailraw", new SimpleStringSchema(), properties))
            .rebalance()
            .map(new CleanFunc())
            .addSink(new JdbcSinkGrabDrive());

    env.execute("runss");
    }

    private static class CleanFunc implements MapFunction<String, Row> {

        @Override
        public Row map(String data) throws Exception {

            Tuple2<Integer,Date> datas =  new Tuple2<Integer, Date>();
            Row datas2 = new Row(5);

            // mengambil harga grabdrive
            String[] splits = data.split("\\|");
            String[] tempdata = splits[1].split("Detail")[0].split("WAKTU")[1].split("\\+0700");
            String[] data1 = splits[0].split("!")[1].split("RP")[1].split(" ");
            Integer harga = Integer.valueOf(data1[1]);

            // mengambil tanggal
            DateFormat formatter;
            Date date;
            formatter = new SimpleDateFormat("dd MMM yy HH:mm");
            date = formatter.parse(tempdata[0]);

            // mengambil layanan
            String[] temp = data.split("Pesanan Jenis Kendaraan:");
            String[] temp2 = temp[1].split("Diterbitkan oleh Pengemudi");
            String layanan = temp2[0];

            // mengambil penjemputan
            String[] tempJemput = data.split("Lokasi Penjemputan:");
            datas.f0 = harga;
            datas.f1 = date;
            if( tempJemput.length > 1 ){
                String jemput = tempJemput[1].split("Lokasi Tujuan:")[0].trim();
                String tujuan = tempJemput[1].split("Lokasi Tujuan:")[1].split("Profil:")[0].trim();
                datas2.setField(0,datas.f0);
                datas2.setField(1, new java.sql.Date(datas.f1.getTime()));
                datas2.setField(2,layanan);
                datas2.setField(3,jemput);
                datas2.setField(4,tujuan);
            }

            return datas2;
        }
    }

}
