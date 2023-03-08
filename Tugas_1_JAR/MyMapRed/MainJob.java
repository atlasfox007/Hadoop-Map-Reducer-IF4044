package MyMapRed;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MainJob {

    public static String youtubeResourceHelper(JsonNode specificData) {
        JsonNode crawler_target = specificData.get("crawler_target");
        if (crawler_target != null && crawler_target instanceof JsonNode) {
            JsonNode specific_resource_type = crawler_target.get("specific_resource_type");
            if (specific_resource_type != null) {
                return specific_resource_type.asText();
            }
        }
        return null;

    }

    public static String twitterResourceHelper(JsonNode specificData) {
        JsonNode crawler_target = specificData.get("crawler_target");
        if (crawler_target != null && crawler_target instanceof JsonNode) {
            JsonNode specific_resource_type = crawler_target.get("specific_resource_type");
            if (specific_resource_type != null) {
                return specific_resource_type.asText();
            }
        }
        return null;
    }

    public static String instagramResourceHelper(JsonNode specificData) {
        JsonNode crawler_target = specificData.get("object");
        if (crawler_target != null && crawler_target instanceof JsonNode) {
            JsonNode specific_resource_type = crawler_target.get("social_media");
            if (specific_resource_type != null) {
                return specific_resource_type.asText();
            }
        }
        return null;
    }

    public static String facebookResourceHelper(JsonNode specificData) {
        JsonNode crawler_target = specificData.get("crawler_target");
        if (crawler_target != null && crawler_target instanceof JsonNode) {
            JsonNode specific_resource_type = crawler_target.get("resource_type");
            if (specific_resource_type != null) {
                return specific_resource_type.asText();
            }
        }
        return null;
    }

    public static String getResource(JsonNode specData) {
        if (twitterResourceHelper(specData) != null) {
            return twitterResourceHelper(specData);

        } else if (facebookResourceHelper(specData) != null) {
            return facebookResourceHelper(specData);

        } else if (instagramResourceHelper(specData) != null) {
            return instagramResourceHelper(specData);
        } else if (youtubeResourceHelper(specData) != null) {
            return youtubeResourceHelper(specData);
        } else {
            return null;
        }
    }

    public static String getDateString(JsonNode json) {
        if (json.has("created_time")) {
            return json.get("created_time").asText();

        } else if (json.has("created_at")) {
            return json.get("created_at").asText();

        } else if (json.has("snippet")) {
            JsonNode snippet = json.get("snippet");
            if (snippet.has("publishedAt")) {
                return snippet.get("publishedAt").asText();

            } else if (snippet.has("topLevelComment") && snippet.get("topLevelComment").has("snippet")) {
                JsonNode topLevelComment = snippet.get("topLevelComment");
                JsonNode topLevelCommentSnippet = topLevelComment.get("snippet");

                if (topLevelCommentSnippet.has("publishedAt")) {
                    return topLevelCommentSnippet.get("publishedAt").asText();
                }
            }
        }
        return null;
    }

    // Mapper class, custom output with DateSocMedPair
    public static class MyMapper extends Mapper<LongWritable, Text, DateSocMedPair, LongWritable> {

        private final ObjectMapper mapper = new ObjectMapper();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Parse the JSON record
            try {
                JsonNode jsonArray = mapper.readTree(value.toString());

                for(JsonNode json : jsonArray) {
                    // Extract the date, social media, and value fields
                    String date = getDateString(json);
                    String soc_med = getResource(json);
                    long val = 1;

                    // Create the output key and value
                    DateSocMedPair outputKey = new DateSocMedPair(date,soc_med);
                    LongWritable outputValue = new LongWritable(val);

                    // Emit the key-value pair
                    context.write(outputKey, outputValue);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Reducer class
    public static class MyReducer extends Reducer<DateSocMedPair, LongWritable, DateSocMedPair, LongWritable> {
        // Reduce function
        public void reduce(DateSocMedPair key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            try {
                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                context.write(key, new LongWritable(sum));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Main function
    public static void main(String args[]) throws Exception {
        try {
            Configuration conf = new Configuration();

            // Set the job name
            Job job = Job.getInstance(conf, "MainJob");
            job.setJarByClass(MainJob.class);

            // Set the mapper and reducer classes
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            // Output from mapper
            job.setMapOutputKeyClass(DateSocMedPair.class);
            job.setMapOutputValueClass(LongWritable.class);

            // output from reducer settings
            job.setOutputKeyClass(DateSocMedPair.class);
            job.setOutputValueClass(LongWritable.class);
            job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

            // Set the input and output paths
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.out.println("Driver error: " + e.toString());
        }
    }
}