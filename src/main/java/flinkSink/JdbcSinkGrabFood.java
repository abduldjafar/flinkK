package flinkSink;

import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.PreparedStatement;

public class JdbcSinkGrabFood extends JdbcSinkGrabDrive {
    private PreparedStatement preparedStatement = null;

    public JdbcSinkGrabFood(){
        this.sql="";
    }

    @Override
    public void invoke(Row row) throws Exception {
        try {
            //4.组装数据，执行插入操作
            preparedStatement.setInt(1, (Integer) row.getField(0));
            preparedStatement.setDate(2, (Date) row.getField(1));
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
