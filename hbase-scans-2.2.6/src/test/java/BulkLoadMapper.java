import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String hbaseTable;
    private String dataSeperator;
    private ImmutableBytesWritable hbaseTableName;

    public BulkLoadMapper() {

    }
    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        hbaseTable = configuration.get("hbase.table.name");
        dataSeperator = ",";
        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        try {
//            System.out.println("******************");
//            System.out.println("inside mapper");
//            System.out.println("******************");
            String[] values = value.toString().split(dataSeperator);
            String rowKey = values[0];
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(TestPassedFakeRowKeysToFilterDuringScans.CF_BYTES, TestPassedFakeRowKeysToFilterDuringScans.CQ_BYTES, Bytes.toBytes(values[1]));
            context.write(hbaseTableName, put);
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}