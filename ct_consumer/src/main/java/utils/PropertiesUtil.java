package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by xz on 2019/2/28.
 */
public class PropertiesUtil {

    public static Properties properties = null;

    static {
        try {
//            InputStream inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties");
            InputStream inputStream = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
