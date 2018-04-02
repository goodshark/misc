package test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Master extends BaseMasterObserver {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        LOG.info("test-only post create table start");
        TableName tableName = desc.getTableName();
        MasterServices services = ctx.getEnvironment().getMasterServices();
        MasterFileSystem masterFileSystem = services.getMasterFileSystem();
        FileSystem fileSystem = masterFileSystem.getFileSystem();

        Path blobPath = new Path(tableName.getQualifierAsString() + "-blobs");
        LOG.info("test-only post create table gen path: " + blobPath.getParent());
        fileSystem.mkdirs(blobPath);
    }
}
