package flinkSink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JdbcSinkGrabDrive extends RichSinkFunction<Row> {
    public Connection connection = null;
    public PreparedStatement preparedStatement = null;
    public String sql = "";

    public JdbcSinkGrabDrive(){
        this.sql = "INSERT INTO grabdrive (harga,tanggal,layanan,penjemputan,tujuan) VALUES (?, ?, ?, ?, ?);";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "org.postgresql.Driver";
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String username = "postgres";
        String password = "toor";
        Class.forName(driver);
        this.connection = DriverManager.getConnection(url, username, password);
        this.preparedStatement = connection.prepareStatement(this.sql);
    }

    @Override
    public void invoke(Row row) throws Exception {
        try {
            //4.组装数据，执行插入操作
            if(
                    row.getField(0) != null && row.getField(1) != null&&
                            row.getField(2) != null && row.getField(3) != null &&
                            row.getField(4) != null
            ){
                this.preparedStatement.setInt(1, (Integer) row.getField(0));
                this.preparedStatement.setDate(2, (Date) row.getField(1));
                this.preparedStatement.setString(3, (String) row.getField(2));
                this.preparedStatement.setString(4, (String) row.getField(3));
                this.preparedStatement.setString(5, (String) row.getField(4));
                this.preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
        //5.关闭连接和释放资源
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.preparedStatement != null) {
            this.preparedStatement.close();
        }
    }

}

/**
 * code ini dicopy dan diedit dari
 * https://github.com/liguohua-bigdata/simple-flink/blob/master/book/stream/customSource/customSourceJava.md
 **/