package main.pack.mapReduce;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;
import java.util.Map;

public class KeywordsMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, LongWritable> {

	private String[] keywords;

	String finalID = null;
	String finalAuthor = null;
	String highestScore = "0"; // Initialize with a very low value

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		keywords = conf.getStrings("myKeywords");
	}

	@Override
	protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		String[] titles = key.datum().get("title").toString().split("\\s+");
		String[] selftexts = key.datum().get("selftext").toString().split("\\s+");
		String[] score = key.datum().get("score").toString().split("\\s+");
		String[] id = key.datum().get("id").toString().split("\\s+");
		String[] authors = key.datum().get("author").toString().split("\\s+");
		String[] vid = key.datum().get("video").toString().split("\\s+");


		for (int i = 0; i < score.length; i++) {
			if (Integer.parseInt(score[i]) > Integer.parseInt(highestScore)) {
				highestScore = score[i];
				finalID = id[i];
				finalAuthor = authors[i];
			}
		}

	

		for (String title : titles) {
			for (String keyword : keywords) {

				if (title.toLowerCase().contains(keyword.toLowerCase())) {
					context.write(new Text(keyword), new LongWritable(1));
				}
			}
		}
		for (String selftext : selftexts) {
			for (String keyword : keywords) {
				if (selftext.toLowerCase().contains(keyword.toLowerCase())) {
					context.write(new Text(keyword), new LongWritable(1));
				}
			}
		}

		for (String isVideo : vid) {
			context.write(new Text("is_video"), new LongWritable(Boolean.parseBoolean(isVideo) ? 1 : 0));
		}

	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// This code will be executed after the last record
		if (finalID != null && finalAuthor != null) {
			context.write(new Text(finalID), new LongWritable(1));
			context.write(new Text(finalAuthor), new LongWritable(1));
			context.write(new Text(highestScore), new LongWritable(1));
		}
	}
}
