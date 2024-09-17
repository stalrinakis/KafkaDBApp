package main.pack.mapReduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeywordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private Set<String> keywordsSet = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String[] keywords = context.getConfiguration().getStrings("myKeywords");
        for (String keyword : keywords) {
            keywordsSet.add(keyword.toLowerCase());
        }
    }

    @Override
    protected void reduce(Text keyword, Iterable<LongWritable> counts, Context context)
            throws IOException, InterruptedException {
        long sum = 0;

        // Sum up the counts for each keyword
        for (LongWritable count : counts) {
            sum += count.get();
        }

        // Emit the keyword and its total count
        context.write(keyword, new LongWritable(sum));
        // Remove the keyword from the set
        keywordsSet.remove(keyword.toString().toLowerCase());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit each remaining keyword with count 0
        for (String keyword : keywordsSet) {
            context.write(new Text(keyword), new LongWritable(0));
        }
    }
}
