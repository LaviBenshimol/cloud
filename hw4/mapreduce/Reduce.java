package wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text word, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<ArrayList<String>> eventList = new ArrayList<ArrayList<String>>();
        for (Text event : values) {
            String line = event.toString();
            String[] words = line.split(",");
            eventList.add(new ArrayList<String>(Arrays.asList(words)));
        }

        // Sort by timestamp
        Collections.sort(eventList, new Comparator<ArrayList<String>>() {
            @Override
            public int compare(ArrayList<String> o1, ArrayList<String> o2) {
                return o1.get(0).compareTo(o2.get(0));
            }
        });

        //Combining the sorted list
        StringBuilder newList = new StringBuilder();
        for (ArrayList<String> event : eventList) {
            newList.append(event.get(0) + ",");
            newList.append(event.get(1) + ",");
            newList.append(event.get(2) + " ");
        }

        context.write(word, new Text(newList.toString()));

    }
}
