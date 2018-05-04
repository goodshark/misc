package norman;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Created by dengrenbo on 18/5/3.
 */
public class BaseIO {
    private void compressInToOut() throws Exception {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        Configuration conf = new Configuration();
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        CompressionOutputStream out = compressionCodec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);
        out.close();
    }

    private void writeSequnceFile(String uri) throws Exception {
        String[] data = {
                "test-1 haha good",
                "test-2 good day",
                "test-3 xixi hoho"
        };
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
        for (int i = 0; i <data.length; i++) {
            key.set(i);
            value.set(data[i]);
            writer.append(key, value);
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("need sequenceFile_path to write");
            System.exit(-1);
        }
        BaseIO baseIO = new BaseIO();
        baseIO.compressInToOut();
        baseIO.writeSequnceFile(args[0]);
    }
}
