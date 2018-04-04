package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;

public class Ddl {
    private final String ZK = "127.0.0.1:2181";
    private final String MASTER = "127.0.0.1:16010";
    private final String TABLE = "test-table-ddl";
    private String[] familyNames = {"f-1", "f-2"};

    private final String ROWKEY = "row";
    private final String COL = "c1";
    private final String VAL = "val";

    private Connection connection;

    private void createTable() throws Exception {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(TABLE))) {
            System.out.println(TABLE + " exists, no need create");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(Bytes.toBytes(familyNames[0]));
        hTableDescriptor.addFamily(hColumnDescriptor1);
        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(Bytes.toBytes(familyNames[1]));
        hTableDescriptor.addFamily(hColumnDescriptor2);
        admin.createTable(hTableDescriptor);
        boolean res = admin.isTableAvailable(TableName.valueOf(TABLE));
        System.out.println(TABLE + " status: " + res);
    }

    private void printTableRegions(String table) throws Exception {
        TableName tableName = TableName.valueOf(table);
        RegionLocator regionLocator = connection.getRegionLocator(tableName);
        Pair<byte[][], byte[][]> pair = regionLocator.getStartEndKeys();
        for (int i = 0; i < pair.getFirst().length; i++) {
            byte[] startKey = pair.getFirst()[i];
            byte[] endKey = pair.getSecond()[i];
            System.out.println(i + " -- start: " +
                    (startKey.length == 8 ? Bytes.toLong(startKey) : Bytes.toStringBinary(startKey)) + ", end: " +
                    (endKey.length == 8 ? Bytes.toLong(endKey) : Bytes.toStringBinary(endKey))
            );
        }
    }

    private void createTableWithRegions() throws Exception {
        Admin admin = connection.getAdmin();
        // use number as split
        String table = "test-table-ddl-2";
        if (admin.tableExists(TableName.valueOf(table))) {
            System.out.println(table + " exists, no need create");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(table));
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(Bytes.toBytes(familyNames[0]));
        hTableDescriptor.addFamily(hColumnDescriptor1);
        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(Bytes.toBytes(familyNames[1]));
        hTableDescriptor.addFamily(hColumnDescriptor2);
        admin.createTable(hTableDescriptor, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
        boolean res = admin.isTableAvailable(TableName.valueOf(table));
        System.out.println(table + " status: " + res);
        printTableRegions(table);
        // use char as split
        String table2 = "test-table-ddl-3";
        if (admin.tableExists(TableName.valueOf(table2))) {
            System.out.println(table2 + " exists, no need create");
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table2));
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(Bytes.toBytes(familyNames[0]));
        tableDescriptor.addFamily(columnDescriptor1);
        HColumnDescriptor columnDescriptor2 = new HColumnDescriptor(Bytes.toBytes(familyNames[1]));
        tableDescriptor.addFamily(columnDescriptor2);
        byte[][] splits = {
                Bytes.toBytes("A"),
                Bytes.toBytes("D"),
                Bytes.toBytes("G"),
                Bytes.toBytes("K"),
                Bytes.toBytes("O"),
                Bytes.toBytes("T")
        };
        admin.createTable(tableDescriptor, splits);
        res = admin.isTableAvailable(TableName.valueOf(table2));
        System.out.println(table2 + " status: " + res);
        printTableRegions(table2);
    }

    private void listTables() throws Exception {
        Admin admin = connection.getAdmin();
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for (HTableDescriptor tab: tableDescriptors) {
            System.out.println("table: " + tab);
        }
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf(TABLE));
        System.out.println("get table: " + hTableDescriptor);
    }

    private void delTable() throws Exception {
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(TABLE))) {
            System.out.println(TABLE + " not exists, no need to delete it");
            return;
        }
        try {
            // table is NOT disable, delete it will throw exception
            System.out.println(TABLE + " disable status: " + admin.isTableDisabled(TableName.valueOf(TABLE)));
            admin.deleteTable(TableName.valueOf(TABLE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        admin.disableTable(TableName.valueOf(TABLE));
        System.out.println(TABLE + " disable status: " + admin.isTableDisabled(TableName.valueOf(TABLE)));
        admin.deleteTable(TableName.valueOf(TABLE));
        System.out.println(TABLE + " available status: " + admin.isTableAvailable(TableName.valueOf(TABLE)));

    }

    private void modifyTable() throws Exception {
        String table = "test-table-ddl-2";
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(table));
        if (tableDescriptor.hasFamily(Bytes.toBytes("m-col"))) {
            System.out.println(table + " already has m-col, no need add new col-family");
            return;
        }
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("m-col");
        tableDescriptor.addFamily(columnDescriptor);
        admin.disableTable(TableName.valueOf(table));
        admin.modifyTable(TableName.valueOf(table), tableDescriptor);
        admin.enableTable(TableName.valueOf(table));
    }

    private void clusterOp() throws Exception {
        Admin admin = connection.getAdmin();
        ClusterStatus status = admin.getClusterStatus();
        System.out.println("Cluster Status:\n--------------");
        System.out.println("HBase Version: " + status.getHBaseVersion());
        System.out.println("Version: " + status.getVersion());
        System.out.println("Cluster ID: " + status.getClusterId());
        System.out.println("Master: " + status.getMaster());
        System.out.println("No. Backup Masters: " +
                status.getBackupMastersSize());
        System.out.println("Backup Masters: " + status.getBackupMasters());

        System.out.println("No. Live Servers: " + status.getServersSize());
        System.out.println("Servers: " + status.getServers());
        System.out.println("No. Dead Servers: " + status.getDeadServers());
        System.out.println("Dead Servers: " + status.getDeadServerNames());
        System.out.println("No. Regions: " + status.getRegionsCount());
        System.out.println("Regions in Transition: " +
                status.getRegionsInTransition());
        System.out.println("No. Requests: " + status.getRequestsCount());
        System.out.println("Avg Load: " + status.getAverageLoad());
        System.out.println("Balancer On: " + status.getBalancerOn());
        System.out.println("Is Balancer On: " + status.isBalancerOn());
        System.out.println("Master Coprocessors: " +
                Arrays.asList(status.getMasterCoprocessors()));
    }

    public static void main(String[] args) throws Exception {
        Ddl test = new Ddl();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", test.ZK);
        test.connection = ConnectionFactory.createConnection(conf);

        test.createTable();
        test.createTableWithRegions();
        test.listTables();
        test.delTable();
        test.modifyTable();
        test.clusterOp();
    }
}
