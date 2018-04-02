package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test extends BaseRegionObserver {
    private static final Logger LOG = LoggerFactory.getLogger(Test.class);
    public static final byte[] FIXED_ROW = Bytes.toBytes("GETTIME");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        LOG.info("test-only start pre get op ...");
        if (Bytes.equals(get.getRow(), FIXED_ROW)) {
            long time = System.currentTimeMillis();
            Cell cell = CellUtil.createCell(get.getRow(), FIXED_ROW, FIXED_ROW, time, KeyValue.Type.Put.getCode(), Bytes.toBytes(time));
            results.add(cell);
            e.bypass();
        } else {
           LOG.info("test-only row not matched");
        }

    }
}
