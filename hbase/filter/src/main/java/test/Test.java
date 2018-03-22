package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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

    private void rowFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                                            new BinaryComparator(Bytes.toBytes(ROWKEY+"-1")));
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("scan result: " + result);
        }
        table.close();
    }

    private void colFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                                                              new BinaryComparator(Bytes.toBytes(COL)));
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("scan result: " + result);
        }

        Get get = new Get(Bytes.toBytes(ROWKEY+"-1"));
        get.setFilter(qualifierFilter);
        Result result = table.get(get);
        System.out.println("get result: " + result);
        table.close();
    }

    private void valFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("-"));
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("scan result: " + result);
        }

        Get get = new Get(Bytes.toBytes(ROWKEY+"-0"));
        get.setFilter(valueFilter);
        Result result = table.get(get);
        System.out.println("get result: " + result);
        table.close();
    }

    private void dependFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));

        // compare (the timestamp of the col in the same-family) with (the timestamp of the family-col that in arg)
        // exclude the family-col in arg self
        Scan scan = new Scan();
        Filter filter = new DependentColumnFilter(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), true);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("true drop scan result: " + result);
        }

        // compare (the timestamp of the col in the same-family) with (the timestamp of the family-col that in arg)
        // include the family-col in arg self
        scan = new Scan();
        filter = new DependentColumnFilter(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), false);
        scan.setFilter(filter);
        scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("false drop scan result: " + result);
        }

        // compare (the timestamp of the col in the same-family) with (the timestamp of the family-col that satisfy value compare in arg) and ...
        // the value compare
        // include the family-col in arg self
        scan = new Scan();
        filter = new DependentColumnFilter(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL), false,
                CompareFilter.CompareOp.EQUAL, new SubstringComparator("-2"));
        scan.setFilter(filter);
        scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println("false drop with compare scan result: " + result);
        }
        table.close();
    }

    // row filter
    private void singleFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyNames[0]), Bytes.toBytes(COL),
                                                    CompareFilter.CompareOp.EQUAL,
                                                    new SubstringComparator("val-"));
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("single filter cell: " + cell);
            }
        }

        table.close();
    }

    // row filter
    private void prefixFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));

        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(ROWKEY));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("prefix filter cell: " + cell);
            }
        }

        Get get = new Get(Bytes.toBytes(ROWKEY+"-0"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println("prefix get result: " + result);
        table.close();
    }

    private void pageFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter filter = new PageFilter(2);
        byte[] lastRow = null;
        // append the 0 byte into row to exclude the start row
        byte[] POSTFIX = new byte[] { 0x00 };
        while (true) {
            System.out.println("page start ...");
            scan.setFilter(filter);
            if (lastRow != null) {
                byte[] newStartRow = Bytes.add(lastRow, POSTFIX);
                System.out.println("new start row: " + Bytes.toString(newStartRow));
                scan.setStartRow(newStartRow);
            }
            ResultScanner scanner = table.getScanner(scan);
            int pageCnts = 0;
            for (Result result: scanner) {
                System.out.println("page result: " + result);
                pageCnts++;
                lastRow = result.getRow();
            }
            if (pageCnts == 0)
                break;
        }
        table.close();
    }

    private void keyOnlyFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter filter = new KeyOnlyFilter();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("key only false filter cell: " + cell);
            }
        }

        filter = new KeyOnlyFilter(true);
        scan.setFilter(filter);
        scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("key only true filter cell: " + cell);
            }
        }
        table.close();
    }

    private void firstKeyOnlyFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter filter = new FirstKeyOnlyFilter();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("first key only filter cell: " + cell);
            }
        }
        table.close();
    }

    private void timestampFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        List<Long> ts = new ArrayList<>();
        ts.add(1521532067740L);
        ts.add(1521617082912L);
        ts.add(200L);
        Filter filter = new TimestampsFilter(ts);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("timestamp filter cell: " + cell);
            }
        }
        table.close();
    }

    private void colPageFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter filter = new ColumnPaginationFilter(1, 1);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("column page filter cell: " + cell);
            }
        }
        table.close();
    }

    private void colPrefixFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes(COL));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("column prefix filter cell: " + cell);
            }
        }
        table.close();
    }

    /*private void skipFilter() throws Exception {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        Scan scan = new Scan();
        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("val-0"));
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("before skip filter cell: " + cell);
            }
        }

        Filter filter = new SkipFilter(valueFilter);
        scan.setFilter(filter);
        scanner = table.getScanner(scan);
        for (Result result: scanner) {
            for (Cell cell: result.rawCells()) {
                System.out.println("after skip filter cell: " + cell);
            }
        }
        table.close();
    }*/



    public static void main(String[] args) throws Exception {
        Test test = new Test();
        Configuration conf = HBaseConfiguration.create();
        System.out.println("scanner timeout: " + conf.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
        System.out.println("scanner caching: " + conf.getLong(HConstants.HBASE_CLIENT_SCANNER_CACHING, -1));
        conf.set("hbase.zookeeper.quorum", test.ZK);
        test.connection = ConnectionFactory.createConnection(conf);

        test.rowFilter();
        System.out.println("row filter done");
        test.colFilter();
        System.out.println("col filter done");
        test.valFilter();
        System.out.println("val filter done");
        test.dependFilter();
        System.out.println("depend filter done");
        test.singleFilter();
        System.out.println("single filter done");
        test.prefixFilter();
        System.out.println("prefix filter done");
        test.pageFilter();
        System.out.println("page filter done");
        test.keyOnlyFilter();
        System.out.println("key only filter done");
        test.firstKeyOnlyFilter();
        System.out.println("first key only filter done");
        test.timestampFilter();
        System.out.println("timestamp filter done");
        test.colPageFilter();
        System.out.println("col page filter done");
        test.colPrefixFilter();
        System.out.println("col prefix filter done");
    }
}
