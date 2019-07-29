package flinkK;

import flinkSink.JdbcSinkGrabDrive;
import flinkSink.JdbcSinkGrabFood;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class StreamingGrabUsage {

    private static Tuple3<Integer,Date,String> getHargaTanggal(String data) throws Exception{
        Tuple3<Integer,Date,String> dataHargaTanggal = new Tuple3<>();

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

        dataHargaTanggal.f0 = harga;
        dataHargaTanggal.f1 = date;
        dataHargaTanggal.f2 = layanan;

        return  dataHargaTanggal;
    }

    public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");

    DataStreamSink<Row> streamGrabDrive = env
            .addSource(new FlinkKafkaConsumer<>("emailraw", new SimpleStringSchema(), properties))
            .rebalance()
            .map(new GetGoDriveData())
            .setParallelism(5)
            .addSink(new JdbcSinkGrabDrive());

    DataStreamSink<Row> StreamGrabFood = env2
            .addSource(new FlinkKafkaConsumer<>("emailraw", new SimpleStringSchema(), properties))
            .rebalance()
            .map(new GetGoFoodData())
            .setParallelism(5)
            .addSink(new JdbcSinkGrabFood());

    // setiap membuat thread harus mengembalikan return
    Thread taskGrabDrive = new Thread(() -> {
            // your code here ...
        try{
            env.execute("grabdrive");
        }catch(Exception e){
            System.out.println(e.toString());
        }
    });

    Thread taskGrabFood = new Thread(() -> {
            // your code here ...
            try{
                env2.execute("GrabFood");            }catch(Exception e){
                System.out.println(e.toString());
            }
    });


    taskGrabDrive.start();
    taskGrabFood.start();

    }

    private static class GetGoDriveData implements MapFunction<String, Row> {

        @Override
        public Row map(String data) throws Exception {

            Tuple2<Integer,Date> datas =  new Tuple2<Integer, Date>();
            Tuple3<Integer,Date,String> hargaTangglLayanan = getHargaTanggal(data);

            Row datas2 = new Row(5);

            // mengambil penjemputan
            String[] tempJemput = data.split("Lokasi Penjemputan:");
            datas.f0 = hargaTangglLayanan.f0;
            datas.f1 = hargaTangglLayanan.f1;
            if( tempJemput.length > 1 ){
                String jemput = tempJemput[1].split("Lokasi Tujuan:")[0].trim();
                String tujuan = tempJemput[1].split("Lokasi Tujuan:")[1].split("Profil:")[0].trim();
                datas2.setField(0,datas.f0);
                datas2.setField(1, new java.sql.Date(datas.f1.getTime()));
                datas2.setField(2,hargaTangglLayanan.f2);
                datas2.setField(3,jemput);
                datas2.setField(4,tujuan);
            }

            return datas2;
        }
    }

    private static class GetGoFoodData implements MapFunction<String, Row> {

        @Override
        public Row map(String data) throws Exception {

            Tuple2<Integer,Date> datas =  new Tuple2<Integer, Date>();
            Tuple3<Integer,Date,String> hargaTangglLayanan = getHargaTanggal(data);

            Row datas2 = new Row(6);

            // mengambil tempat pembelian
            String[] tempJemput = data.split("Pesanan Dari:");
            if( tempJemput.length > 1 ){

                String tempatPembelian = tempJemput[1].split("Lokasi Pengantaran:")[0];
                String tempatTujuan = tempJemput[1]
                        .split("Lokasi Pengantaran:")[1]
                        .split("Detail Tagihan Detail Pembayaran:")[0];

                String detailPembelian = tempJemput[1].split("Lokasi Pengantaran:")[1]
                        .split("Detail Tagihan Detail Pembayaran:")[1]
                        .split("Pembayaran Jumlah:")[1]
                        .split("Help Centre Ada")[0];
                datas.f0 = hargaTangglLayanan.f0;
                datas.f1 = hargaTangglLayanan.f1;

                datas2.setField(0,datas.f0);
                datas2.setField(1, new java.sql.Date(datas.f1.getTime()));
                datas2.setField(2,hargaTangglLayanan.f2);
                datas2.setField(3,tempatPembelian);
                datas2.setField(4,tempatTujuan);
                datas2.setField(5,detailPembelian);
            }

            return datas2;
        }
    }

}
