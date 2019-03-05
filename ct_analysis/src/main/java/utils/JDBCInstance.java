package utils;

import java.sql.Connection;
import java.sql.SQLException;

public class JDBCInstance {
    private static Connection connection = null;
    private JDBCInstance(){}
    public static Connection getInstance(){
        try {
            if(connection == null || connection.isClosed()){
                connection = JDBCUtil.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
