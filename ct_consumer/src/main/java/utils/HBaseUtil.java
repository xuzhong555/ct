package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by xz on 2019/3/4.
 */
public class HBaseUtil {

    /**
     * 判断表是否存在
     * @param conf
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExitTable(Configuration conf,String tableName) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        connection.close();
        return exists;
    }

    /**
     * 创建命名空间
     * @param conf
     * @param namespace
     * @throws IOException
     */
    public static void initNameSpace(Configuration conf,String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME",String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR","xz").build();

        admin.createNamespace(nd);

        admin.close();
        connection.close();
    }

    /**
     * 创建表,还没有完成预分区操作
     * @param conf
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Configuration conf,int regions,String tableName,String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if(!isExitTable(conf,tableName))return;

        HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
            hd.addFamily(new HColumnDescriptor(cf));
        }
        hd.addCoprocessor("hbase.CalleeWriteObserver");
        admin.createTable(hd,genSplitKeys(regions));

        admin.close();
        connection.close();
    }

    private static byte[][] genSplitKeys(int regions){
        //定义一个存放分区键的数组
        String[] keys = new String[regions];
        //这里默认不会超过两位数的分区，如果超过，需要变更设计，如果需要灵活操作，也需要变更设计
        DecimalFormat decimalFormat = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            keys[i] = decimalFormat.format(i) + "|";
        }

        byte[][] splitKeys = new byte[regions][];
        //生成byte类型的分区键时保证其有序
        TreeSet<byte[]> treeSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }
        int index = 0;
        Iterator<byte[]> iterator = treeSet.iterator();
        while(iterator.hasNext()){
            byte[] next = iterator.next();
            splitKeys[index++] = next;
        }
        return splitKeys;
    }

    /**
     * 生成rowkey
     * regionCode_call1_buildTime_call2_duration
     * @return
     */
    public static String genRowkey(String regionCode,String call1,String buildTime,String call2, String flag, String duration){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(regionCode + "-")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 + "_")
                .append(flag + "_")
                .append(duration);
        return stringBuilder.toString();
    }

    /**
     * 生成分区号
     * @param call1
     * @param buildTime
     * @param regions
     * @return
     */
    public static String genRegionCode(String call1, String buildTime, int regions){
        int len = call1.length();
        //取出后四位号码
        String last4PhoneNum = call1.substring(len - 4);
        //取出年月
        String ym = buildTime
                .replaceAll("-","")
                .replaceAll(":","")
                .replaceAll(" ","")
                .substring(0,6);
        //离散操作
        int hashCode = Integer.valueOf(last4PhoneNum) ^ Integer.valueOf(ym) % regions;
        return new DecimalFormat("00").format(hashCode);

    }


    public static void main(String[] args) {
//        genSplitKeys(6);
    }
}
