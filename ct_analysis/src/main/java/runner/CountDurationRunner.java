package runner;

import kv.key.ComDimension;
import kv.value.CountDurationValue;
import mapper.CountDurationMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducer.CountDurationReducer;

import java.io.IOException;

/**
 * Created by xz on 2019/3/6.
 */
public class CountDurationRunner implements Tool{
    private Configuration configuration;

    public int run(String[] strings) throws Exception {
        //得到conf
        //实例化job
        Job job = Job.getInstance(configuration);
        job.setJarByClass(CountDurationRunner.class);
        //组装Mapper，直接组装了InputFormat
        initHBaseInputConfig(job);
        //组装reduce，需自己组装OutputFormat
        initReducerOutputConfig(job);
        job.setReducerClass(CountDurationReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void initReducerOutputConfig(Job job) {
        job.setReducerClass(CountDurationReducer.class);
        job.setOutputKeyClass(ComDimension.class);
        job.setOutputValueClass(CountDurationValue.class);
        job.setOutputFormatClass(null);
    }

    private void initHBaseInputConfig(Job job) {
        String tableName = "ns_ct:calllog";
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            if(!admin.tableExists(TableName.valueOf(tableName))) throw new RuntimeException("table does not exist");
            Scan scan = new Scan();
            TableMapReduceUtil.initTableMapperJob(
                    tableName,
                    scan,
                    CountDurationMapper.class,
                    ComDimension.class,
                    Text.class,
                    job,
                    true
            );
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(admin != null){
                    admin.close();
                }
                if(connection != null){
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void setConf(Configuration configuration) {
        this.configuration = HBaseConfiguration.create(configuration);
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new CountDurationRunner(), args);
            System.exit(status);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
