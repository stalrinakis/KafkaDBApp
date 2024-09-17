package main.pack.schema;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class RedditPostSchema {

    public static final Schema SCHEMA = new Schema.Parser().parse(
        "{" +
            "\"type\": \"record\"," +
            "\"name\": \"RedditPost\"," +
            "\"fields\": [" +
                "{\"name\": \"title\", \"type\": \"string\"}," +
                "{\"name\": \"url\", \"type\": \"string\"}," +
                "{\"name\": \"subreddit\", \"type\": \"string\"}," +
                "{\"name\": \"id\", \"type\": \"string\"}," +
                "{\"name\": \"score\", \"type\": \"string\"}," +
                "{\"name\": \"author\", \"type\": \"string\"}," +
                "{\"name\": \"selftext\", \"type\": [\"string\", \"null\"]}," +
                "{\"name\": \"subredditSubscribers\", \"type\": \"string\"}," +
                "{\"name\": \"numComments\", \"type\": \"string\"}," +
                "{\"name\": \"upvoteRatio\", \"type\": \"string\"}," +
                "{\"name\": \"video\", \"type\": \"boolean\"}" +
            "]" +
        "}");

    private final GenericRecord record;

    public RedditPostSchema(GenericRecord record) {
        this.record = record;
    }

    // Method to get the post text
    public String getSelfText() {
        return (String) record.get("selftext");
    }
    public String getTitle() {
        return (String) record.get("title");
    }
}
