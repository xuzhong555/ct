package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by xz on 2019/3/5.
 */
public class ConnectionInstance {

    private static Connection connection;

    public static synchronized Connection getConnection(Configuration conf){

        if(connection == null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

}
