package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] lineSplit = line.split("\t");
        String[] events = lineSplit[1].split(" ");

        // Removing the timestamp from the list.
        String str = "";
        for (String event : events) {
            String[] eventSplit = event.split(",");
            str = str + eventSplit[1] + "," + eventSplit[2] + " ";
        }

        context.write(new Text(lineSplit[0]), new Text(str));

    }
}
