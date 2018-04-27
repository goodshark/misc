package mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BaseReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemperatue = Integer.MIN_VALUE;
        for (IntWritable temp: values) {
            if (temp.get() > maxTemperatue)
                maxTemperatue = temp.get();
        }
        context.write(key, new IntWritable(maxTemperatue));
    }
}
