package norman;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
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
        OutputStream out = fs.create(new Path(dst));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("need path arg: readData source destination");
            System.exit(-1);
        }

        BaseFs baseFs = new BaseFs();

        baseFs.copyData(args[0]);
        baseFs.createFile(args[1], args[2]);
    }
}
