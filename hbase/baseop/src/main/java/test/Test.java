package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


public class Test {
    private final String ZK = "127.0.0.1:2181";
    private final String MASTER = "127.0.0.1:16010";
    private final String TABLE = "test-table";
    private String[] familyNames = {"f-1", "f-2"};

    private final String ROWKEY = "row";
    private final String COL = "c1";
    private final String VAL = "val";

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
        Put put = new Put(Bytes.toBytes(ROWKEY+"-1"));
        put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-singleData"));
        table.put(put);
        table.close();
    }

    private void insertBunchData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Put put = new Put(Bytes.toBytes(ROWKEY+"-"+Integer.toString(i)));
            put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-"+Integer.toString(i)));
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    // atom insert operation only allow same row
    private void atomInsertData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Put put = new Put(Bytes.toBytes(ROWKEY+"-atom"));
        put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-new"));
        // when row-family:col is null, then insert the put
        boolean res = table.checkAndPut(Bytes.toBytes(ROWKEY+"-atom"), Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), null, put);
        System.out.println("when old col is null, atom op status: " + res);

        put = new Put(Bytes.toBytes(ROWKEY+"-atom"));
        put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-new-2"));
        res = table.checkAndPut(Bytes.toBytes(ROWKEY+"-atom"), Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-new"), put);
        System.out.println("when old col is VAL-new, atom op status: " + res);

        put = new Put(Bytes.toBytes(ROWKEY+"-atom"));
        put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-foobar"));
        res = table.checkAndPut(Bytes.toBytes(ROWKEY+"-atom"), Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes(VAL+"-hehe"), put);
        System.out.println("when old col is VAL-hehe, atom op status: " + res);

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

        test.createTable();
        System.out.println("create table done");
        test.insertSingleData();
        System.out.println("insert single data done");
        test.insertBunchData();
        System.out.println("insert bunch data done");
        test.atomInsertData();
        System.out.println("check and put data done");

        test.postOp();
    }
}
