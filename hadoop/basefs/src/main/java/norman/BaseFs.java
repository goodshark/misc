package norman;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by dengrenbo on 18/4/29.
 */
public class BaseFs {

    private void copyData(String uri) throws Exception {
        Configuration conf = new Configuration();
        // if uri's schema is empty, will use (fs.defaultFs in core-site.xml) conf
        // example, uri: /x/y/z, fs will combine conf and path into hdfs://localhost:9000/x/y/z
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            if (!fs.exists(new Path(uri))) {
                System.err.println(uri + " is not exists");
                System.exit(-1);
            }
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    private void createFile(String src, String dst) throws Exception {
        Configuration conf = new Configuration();
        // if uri's schema is empty, will use (fs.defaultFs in core-site.xml) conf
        // example, uri: /x/y/z, fs will combine conf and path into hdfs://localhost:9000/x/y/z
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        InputStream in = new BufferedInputStream(new FileInputStream(src));
        // create will auto create parent directory(if it do not exist)
        // also FileSystem can use mkdirs to create a directory explicity
        OutputStream out = fs.create(new Path(dst));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    private void getMetaInfo(String uri) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus fileStatus = fs.getFileStatus(new Path(uri));
        System.out.println("is directory: " + fileStatus.isDirectory());
        System.out.println("path: " + fileStatus.getPath());
        System.out.println("replication: " + fileStatus.getReplication());
        System.out.println("owner: " + fileStatus.getOwner());
    }

    private void listFiles(String uri) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        // can add PathFilter to filter result
        FileStatus[] fileStatuses = fs.listStatus(new Path(uri));
        for (FileStatus fileStatus: fileStatuses) {
            System.out.println("path in " + uri + ": " + fileStatus.getPath());
        }
    }

    private void delFile(String uri) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        boolean res = fs.delete(new Path(uri), false);
        if (res)
            System.out.println("delete " + uri + " success");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("need path arg: readData source destination metaFile");
            System.exit(-1);
        }

        BaseFs baseFs = new BaseFs();

        baseFs.copyData(args[0]);
        baseFs.createFile(args[1], args[2]);
        baseFs.getMetaInfo(args[0]);
        baseFs.listFiles(args[3]);
        baseFs.delFile(args[2]);
    }
}
