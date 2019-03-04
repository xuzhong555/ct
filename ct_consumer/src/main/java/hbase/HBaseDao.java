package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by xz on 2019/3/4.
 */
public class HBaseDao {

    private int regions;
    private String nameSpace;
    private String tableName;
    public static final Configuration conf;
    private Table table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");;
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");;

    static {
        conf = HBaseConfiguration.create();
    }

    /**
     * hbase.calllog.regions=6
     hbase.calllog.namespace=ns_ct
     hbase.calllog.tablename=ns_ct:calllog
     */
    public HBaseDao() {
        try {
            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
            nameSpace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));

            if(!HBaseUtil.isExitTable(conf,tableName)){
                HBaseUtil.initNameSpace(conf,nameSpace);
                HBaseUtil.createTable(conf,regions,tableName,"f1","f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 15596505995,17519874292,2017-03-11 00:30:19,0652
     * 将当前数据 put 到 HTable 中
     * @param value
     */
    public void put(String value) {

        try {
            String[] split = value.split(".");
            String caller = split[0];
            String callee = split[1];
            String buildTime = split[2];
            String duration = split[3];
            String regionCode = HBaseUtil.genRegionCode(caller,buildTime,regions);

            String buildTimeFormat = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime());

            String rowkey = HBaseUtil.genRowkey(regionCode, caller, buildTimeFormat, callee, "1", duration);

            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cell1"),Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cell2"),Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buildTimeTs"),Bytes.toBytes(buildTimeTs));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("flag"),Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("duration"),Bytes.toBytes(duration));

            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
