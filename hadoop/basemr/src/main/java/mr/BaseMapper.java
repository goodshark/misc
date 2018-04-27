package mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BaseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int YEAR_START_POSITION = 15;
    private static final int YEAR_WIDTH = 4;
    private static final int TEMPERA_START_POSITION = 87;
    private static final int TEMPERA_WIDTH = 5;
    private static final int QUAILTY_START_POSITION = 92;
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(YEAR_START_POSITION, YEAR_START_POSITION+YEAR_WIDTH);
        int temperatue;
        if (line.charAt(TEMPERA_START_POSITION) == '+') {
            temperatue = Integer.parseInt(line.substring(TEMPERA_START_POSITION+1, TEMPERA_START_POSITION+TEMPERA_WIDTH));
        } else {
            temperatue = Integer.parseInt(line.substring(TEMPERA_START_POSITION, TEMPERA_START_POSITION+TEMPERA_WIDTH));
        }
        String quality = line.substring(QUAILTY_START_POSITION, QUAILTY_START_POSITION+1);
        // check temperatue and quality is legal
        if (temperatue != MISSING && quality.matches("[01459]"))
            context.write(new Text(year), new IntWritable(temperatue));
    }
}
