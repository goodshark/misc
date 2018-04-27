package norman;

import mr.BaseMapper;
import mr.BaseReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BaseMr {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE: need INPUT_PATH OUTPUT_PATH");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(BaseMr.class);
        job.setJobName("getMaxTemperature");

        // can call many times for add many different input paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // can only call once, there is only one output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BaseMapper.class);
        job.setReducerClass(BaseReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
