package flinkSink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import javax.validation.constraints.Null;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JdbcSinkGrabDrive extends RichSinkFunction<Row> {
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
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
        connection = DriverManager.getConnection(url, username, password);
        preparedStatement = connection.prepareStatement(this.sql);
    }

    @Override
    public void invoke(Row row) throws Exception {
        try {
            //4.组装数据，执行插入操作
            if(
                    row.getField(0) != null && row.getField(2) != null&&
                            row.getField(2) != null && row.getField(3) != null &&
                            row.getField(4) != null
            ){
                preparedStatement.setInt(1, (Integer) row.getField(0));
                preparedStatement.setDate(2, (Date) row.getField(1));
                preparedStatement.setString(3, (String) row.getField(2));
                preparedStatement.setString(4, (String) row.getField(3));
                preparedStatement.setString(5, (String) row.getField(4));
                preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
        //5.关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

}

/**
 * code ini dicopy dan diedit dari
 * https://github.com/liguohua-bigdata/simple-flink/blob/master/book/stream/customSource/customSourceJava.md
 **/