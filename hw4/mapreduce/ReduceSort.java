package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceSort extends Reducer<IntWritable, Text, Text, Long> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long keyChange = key.get();
        keyChange = keyChange*(-1);
        for (Text value : values) {
            context.write(new Text(value), keyChange);
        }
    }
}
