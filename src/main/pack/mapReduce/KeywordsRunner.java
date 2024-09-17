package main.pack.mapReduce;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KeywordsRunner {
	
	public static void RunnerMain(String[] args, String[] keywords) throws Exception {

    	Configuration conf = new Configuration();
    	conf.setStrings("myKeywords", keywords);
    	
        Job job = Job.getInstance(conf, "Avro MapReduce Job");

        job.setJarByClass(KeywordsRunner.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroKeyInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(KeywordsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(KeywordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        try {
            job.waitForCompletion(true);
            System.out.println("Job was successful!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Job was not successful!");
        }
    }

}
