package flinkSink;

import org.apache.flink.types.Row;
import java.sql.Date;

public class JdbcSinkGrabFood extends JdbcSinkGrabDrive {

    public JdbcSinkGrabFood(){
        sql="INSERT INTO grabfood (harga,tanggal,layanan,pembelian,tujuan,detail) VALUES (?, ?, ?, ?, ?,?);";
    }

    @Override
    public void invoke(Row row)  {
        try {
            //4.组装数据，执行插入操作
            if(
                    row.getField(0) != null && row.getField(1) != null&&
                            row.getField(2) != null && row.getField(3) != null &&
                            row.getField(4) != null && row.getField(5) != null
            ){
                preparedStatement.setInt(1, (Integer) row.getField(0));
                preparedStatement.setDate(2, (Date) row.getField(1));
                preparedStatement.setString(3, (String) row.getField(2));
                preparedStatement.setString(4, (String) row.getField(3));
                preparedStatement.setString(5, (String) row.getField(4));
                preparedStatement.setString(6, (String) row.getField(5));
                preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
