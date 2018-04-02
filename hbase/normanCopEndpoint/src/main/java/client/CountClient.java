package client;

import coprocessor.generated.RowCounterProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class CountClient {
    private final String ZK = "127.0.0.1:2181";
    private final String MASTER = "127.0.0.1:16010";
    private final String TABLE = "test-table";
    private String[] familyNames = {"f-1", "f-2"};

    private final String ROWKEY = "row";
    private final String COL = "c1";
    private final String VAL = "val";

    private Connection connection;

    private void countRows() throws Throwable {
        Table table = connection.getTable(TableName.valueOf(TABLE));
        final RowCounterProtos.CountRequest request = RowCounterProtos.CountRequest.getDefaultInstance();

        Batch.Call<RowCounterProtos.RowCountService, Long> rpcCall = new Batch.Call<RowCounterProtos.RowCountService, Long>() { // co EndpointExample-3-Batch Create an anonymous class to be sent to all region servers.
            public Long call(RowCounterProtos.RowCountService counter)
                    throws IOException {
                BlockingRpcCallback<RowCounterProtos.CountResponse> rpcCallback =
                        new BlockingRpcCallback<RowCounterProtos.CountResponse>();
                counter.getRowCount(null, request, rpcCallback); // co EndpointExample-4-Call The call() method is executing the endpoint functions.
                RowCounterProtos.CountResponse response = rpcCallback.get();
                return response.hasCount() ? response.getCount() : 0;
            }
        };

        Map<byte[], Long> results = table.coprocessorService(
                RowCounterProtos.RowCountService.class, // Define the protocol interface being invoked.
                null, null, // Set start and end row key to "null" to count all rows.
                rpcCall
        );

        long total = 0;
        for (Map.Entry<byte[], Long> entry: results.entrySet()) {
            total += entry.getValue();
            System.out.println("region: " + Bytes.toString(entry.getKey()) + ", counts: " + entry.getValue());
        }
        System.out.println("total counts: " + total);
    }

    public static void main(String[] args) throws Throwable {
        CountClient test = new CountClient();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", test.ZK);
        test.connection = ConnectionFactory.createConnection(conf);
        test.countRows();
    }
}
