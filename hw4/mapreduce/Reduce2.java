package wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            String line = value.toString();
            String[] events = line.split(" ");

            if (events.length > 3) {
                for(int i = 0; i < events.length - 2; i++) {
                    // No need to duplicate any event
                    context.write(new Text("("+events[i]+");"+"("+events[i+1]+");"+"("+events[i+2]+")"), key);
                }
            }
            else if (events.length == 2) {
                // need to duplicate the second event
                String[] eventSplit = events[1].split(",");
                // Changing the duplicate PI to zero
                eventSplit[1] = "0";
                context.write(new Text("("+events[0]+");"+"("+events[1]+")"+"("+eventSplit[0]+","+eventSplit[1]+")"), key);
            } else if (events.length == 1) {
                // Need to duplicate this event twice
                String[] eventSplit = events[0].split(",");
                // Changing the duplicate PI to zero
                eventSplit[1] = "0";
                context.write(new Text("("+events[0]+");"+"("+eventSplit[0]+","+eventSplit[1]+")';'"+"("+eventSplit[0]+","+eventSplit[1]+")"), key);
            } else {
                context.write(new Text("("+events[0]+");"+"("+events[1]+");"+"("+events[2]+")"), key);
            }
        }
    }
}

