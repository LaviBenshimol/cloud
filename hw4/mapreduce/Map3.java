package wordcount;

import java.lang.String;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map3 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(" ");
        String[] events = words[0].split(";");

        // Split the two event two two different events
        String[] event1 = events[1].split(",");
        String[] event2 = events[2].split(",");

        int len = events[2].length();
        if(events[2].charAt(len - 1) == '1') {
            context.write(new Text("("+event1[0].substring(1,event1[0].length()-1)+ ","+event2[0].substring(1,event2[0].length()-1)+")"), new IntWritable(1));
        }
    }
}
