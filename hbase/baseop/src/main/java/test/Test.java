package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class Test {
    private final String ZK = "127.0.0.1:2181";
    private final String MASTER = "127.0.0.1:16010";
    private final String TABLE = "test-table";
    private String[] familyNames = {"f-1", "f-2"};

    private final String ROWKEY = "row-1";
    private final String COL = "c1";
    private final String VAL = "val-1";

    private Connection connection;

    private void createTable() throws Exception {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(TABLE))) {
            System.out.println(TABLE + " already exists");
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        admin.createTable(tableDescriptor);
    }

    private void insertSingleData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Put put = new Put(Bytes.toBytes(ROWKEY));
        put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL));
        table.put(put);
        table.close();
    }

    private void postOp() throws Exception {
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        Test test = new Test();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", test.ZK);
//        conf.set("hbase.master", test.MASTER);
        test.connection = ConnectionFactory.createConnection(conf);

        System.out.println("start create table");
        test.createTable();
        System.out.println("start insert data");
        test.insertSingleData();

        test.postOp();
    }
}
