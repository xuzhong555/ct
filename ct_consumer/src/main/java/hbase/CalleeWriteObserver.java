package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by xz on 2019/3/4.
 */
public class CalleeWriteObserver extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);

        //1、获取需要操作的表
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        //2、获取当前操作的表
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        //3、判断需要操作的表是否就是当前表，如果不是，则 ret
        if(!targetTableName.equals(currentTableName))return;
        //4、得到当前插入数据的值并封装新的数据，oriRowkey 举例：01_15369468720_20170727081033_13720860202_1_0180
        String oriRowkey = Bytes.toString(put.getRow());
        //5、拆分
        String[] splits = oriRowkey.split("_");

        //如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据)
        String flag = splits[4];
        if(flag.equals("0"))return;

        String caller = splits[1];
        String callee = splits[3];
        String buildTime = splits[2];
        String newflag = "0";
        String duration = splits[5];

        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        String regionCode = HBaseUtil.genRegionCode(callee, buildTime, regions);

        String rowkey = HBaseUtil.genRowkey(regionCode, callee, buildTime, callee, newflag, duration);
        String buildTimeTs = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            buildTimeTs = "";
            e1.printStackTrace();
        }
        Put newPut = new Put(Bytes.toBytes(rowkey));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cell1"),Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cell2"),Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buildTimeTs"),Bytes.toBytes(buildTimeTs));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("flag"),Bytes.toBytes(newflag));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        Table table = e.getEnvironment().getTable(TableName.valueOf(currentTableName));
        table.put(newPut);

        table.close();

    }
}
