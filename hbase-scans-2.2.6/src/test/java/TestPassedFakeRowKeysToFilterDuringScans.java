import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPassedFakeRowKeysToFilterDuringScans {

    private static HBaseTestingUtility hBaseTestingUtility;
    private static Configuration configuration;
    public static final String CF = "CF1";
    public static final String CQ = "CQ1";
    public static final byte[]  CF_BYTES = Bytes.toBytes(CF);
    public static final byte[] CQ_BYTES = Bytes.toBytes(CQ);
    public static Table table;
    public static final TableName TABLE_NAME = TableName.valueOf("testScans");

    @BeforeClass
    public static void setup() throws Exception{
        final Configuration startingConf = new Configuration();
        startingConf.set("hbase.regionserver.codecs", "gz");
        startingConf.setStrings(
                HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS,
                "org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner");

        startingConf.setInt("hbase.client.retries.number", 3);
        startingConf.setInt("hbase.client.pause", 1000);

        hBaseTestingUtility = new HBaseTestingUtility(startingConf);

        hBaseTestingUtility.startMiniCluster();

        configuration = hBaseTestingUtility.getConfiguration();

        table = hBaseTestingUtility.createTable(TABLE_NAME, CF);

        loadDataUsingPuts();
        //bulkload();
    }

    @Test
    public void testScanWithFilterAmbiguousRow() throws Exception {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes("1"));
            scan.addColumn(CF_BYTES, CQ_BYTES);
            scan.setFilter(new KeyOnlyFilter());

            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner) {
                System.out.println("*****************");
                System.out.println("Retrieved row keys on client side: " + Bytes.toStringBinary(result.getRow()));
                System.out.println("*****************");
            }
            scanner.close();
    }

    @After
    public void afterTest() throws Exception{
        table.close();
    }

    @AfterClass
    public static void teardown() throws Exception{
        hBaseTestingUtility.shutdownMiniCluster();
    }

    private static void bulkload() throws Exception {
        FileSystem fileSystem = hBaseTestingUtility.getTestFileSystem();

        ClassLoader classLoader = TestPassedFakeRowKeysToFilterDuringScans.class.getClassLoader();
        String path = new File(classLoader.getResource("inputs.txt").getFile()).getPath();
        String folderName = "/tmp/"+ UUID.randomUUID();
        Path outputPath = new Path(folderName);

        fileSystem.copyFromLocalFile(new Path(path), new Path(path));
        Configuration configuration = new Configuration(hBaseTestingUtility.getConfiguration());
        configuration.set("hbase.table.name", TABLE_NAME.getNameAsString());
        Job job = Job.getInstance(configuration);
        job.setJarByClass(BulkLoadMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapperClass(BulkLoadMapper.class);
        FileInputFormat.addInputPaths(job, path);
        FileSystem.getLocal(configuration).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapOutputValueClass(Put.class);
        try(Connection connection = ConnectionFactory.createConnection(configuration)) {
            ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(CF_BYTES).build();
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(cf1).build();
            HFileOutputFormat2.configureIncrementalLoad(job, tableDescriptor, connection.getRegionLocator(TABLE_NAME));
            job.waitForCompletion(true);

            if (!job.isSuccessful()) {
                Assert.fail("failed occurred while generating HFiles");
            }

            BulkLoadHFilesTool bulkLoadHFilesTool = new BulkLoadHFilesTool(configuration);
            bulkLoadHFilesTool.bulkLoad(TABLE_NAME, outputPath);
        }
    }

    private static void loadDataUsingPuts() throws Exception{
        Put put = new Put(Bytes.toBytes("0"));
        put.addColumn(CF_BYTES, CQ_BYTES, Bytes.toBytes("row1"));

        Put put2 = new Put(Bytes.toBytes("2"));
        put2.addColumn(CF_BYTES, CQ_BYTES, Bytes.toBytes("row2"));


        Put put3 = new Put(Bytes.toBytes("3"));
        put3.addColumn(CF_BYTES, CQ_BYTES, Bytes.toBytes("row3"));

        List<Put> puts = Arrays.asList(put, put2, put3);

        table.put(puts);
    }
}
