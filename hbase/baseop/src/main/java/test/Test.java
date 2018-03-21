package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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

    private void getData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Get get = new Get(Bytes.toBytes(ROWKEY+"-1"));
        get.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        Result result = table.get(get);
        byte[] val = result.getValue(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        System.out.println("get value: " + Bytes.toString(val));
        table.close();
    }

    private void getListData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        List<Get> gets = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Get get = new Get(Bytes.toBytes(ROWKEY+"-"+Integer.toString(i)));
            get.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes("c1"));
            gets.add(get);
        }
        Result[] results = table.get(gets);
        for (int i = 0; i < results.length; i++)
            System.out.println(results[i]);

        // get whole row
        Get get = new Get(Bytes.toBytes(ROWKEY+"-"+Integer.toString(0)));
        Result result = table.get(get);
        System.out.println(result);

        table.close();
    }

    private void deleteData() throws Exception {
        // delete latest version, older version will stay
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Delete delete = new Delete(Bytes.toBytes(ROWKEY+"-0"));
        delete.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        table.delete(delete);
        table.close();
    }

    private void deleteListData() throws Exception {
        // delete latest version, older version will stay
        Table table = connection.getTable(TableName.valueOf(TABLE));
        List<Delete> deletes = new ArrayList<>();
        Delete delete = new Delete(Bytes.toBytes(ROWKEY+"-1"));
        deletes.add(delete);
        delete = new Delete(Bytes.toBytes(ROWKEY+"-2"));
        delete.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        deletes.add(delete);
        // delete all version
        delete = new Delete(Bytes.toBytes(ROWKEY+"-atom"));
        delete.addColumns(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));

        table.delete(delete);
        table.close();
    }

    // same as checkAndPut
    private void checkAndDelete() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
    }

    private void batchOperation() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        List<Row> batchs = new ArrayList<>();
        Put put = new Put(Bytes.toBytes(ROWKEY+"-foobar"));
        put.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), Bytes.toBytes("jim"));
        batchs.add(put);
        Get get = new Get(Bytes.toBytes(ROWKEY+"-1"));
        get.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        batchs.add(get);
        // this Get has wrong column family, need catch exception while batching
        get = new Get(Bytes.toBytes(ROWKEY+"-1"));
        get.addFamily(Bytes.toBytes("wrong"));
        batchs.add(get);

        Object[] results = new Object[batchs.size()];
        try {
            table.batch(batchs, results);
        } catch (Exception e) {
            System.out.println(e);
        }

        // the 3rd result will print exception that has noColumnFamily
        for (int i = 0; i < results.length; i++)
            System.out.println("index " + Integer.toString(i) + ": " + results[i].getClass() + ", has: " + results[i]);
        table.close();
    }

    private void scanData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("========= scan all rows =========");
        for (Result result: scanner) {
            System.out.println("result: " + result);
        }

        scan = new Scan();
        scan.addFamily(Bytes.toBytes(familyNames[0]));
        scanner = table.getScanner(scan);
        System.out.println("========= scan one family-column rows =========");
        for (Result result: scanner) {
            System.out.println("result: " + result);
        }

        scan = new Scan();
        scan.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        scanner = table.getScanner(scan);
        System.out.println("========= scan one CELL rows =========");
        for (Result result: scanner) {
            System.out.println("result: " + result);
        }

        scan = new Scan();
        scan.setStartRow(Bytes.toBytes(ROWKEY+"-0"));
        scan.setStopRow(Bytes.toBytes(ROWKEY+"-2"));
        scan.addColumn(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL));
        scanner = table.getScanner(scan);
        System.out.println("========= scan one CELL between these rows =========");
        for (Result result: scanner) {
            System.out.println("result: " + result);
        }
    }

    private void scanCacheData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        scan.setCaching(5);
        scan.setBatch(2);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("result: " + result);
        }
        table.close();
    }

    private void postOp() throws Exception {
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        Test test = new Test();
        Configuration conf = HBaseConfiguration.create();
        System.out.println("scanner timeout: " + conf.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
        System.out.println("scanner caching: " + conf.getLong(HConstants.HBASE_CLIENT_SCANNER_CACHING, -1));
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
        test.getData();
        System.out.println("get data done");
        test.getListData();
        System.out.println("get list data done");
        test.deleteData();
        System.out.println("delete data done");
        test.deleteListData();
        System.out.println("delete list data done");
        test.batchOperation();
        System.out.println("batch handle data done");
        test.scanData();
        System.out.println("scan data done");
        test.scanCacheData();
        System.out.println("scan cache data done");

        test.postOp();
    }
}
