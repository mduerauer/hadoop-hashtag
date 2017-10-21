package at.ac.is161505.hashtag;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by n17405180 on 21.10.17.
 */
public class HashtagCount1c extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagCount1c.class);

    private static final int TOP_N = 50;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HashtagCount1c(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        String outputDir = args[1];

        /**
         * JOB 1 counts the hashtags
         */

        Job job1 = Job.getInstance(getConf(), "hashtagcount");
        Path job1Output = new Path(outputDir + "/job1");
        job1.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, job1Output);

        job1.setMapperClass(HashtagCount1a.Map.class);
        job1.setReducerClass(HashtagCount1a.Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        int job1Ok = job1.waitForCompletion(true) ? 0 : 1;

        if(job1Ok != 0) {
            return job1Ok;
        }

        /**
         * JOB 2 extracts the TOP hashtags
         */

        Job job2 = Job.getInstance(getConf(), "hashtagcount_n");
        Path job2Output = new Path(outputDir + "/job2");
        job2.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job2, job1Output);
        FileOutputFormat.setOutputPath(job2, job2Output);

        job2.setMapperClass(HashtagCount1b.TopNMap.class);
        job2.setReducerClass(HashtagCount1b.TopNReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        int job2Ok = job2.waitForCompletion(true) ? 0 : 1;

        if(job2Ok != 0) {
            return job2Ok;
        }

        /**
         * JOB 3 extracts the user that used a specific hash tag the most
         */

        Job job3 = Job.getInstance(getConf(), "hashtagtouser");
        Path job3Output = new Path(outputDir + "/job3");
        job3.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job3, inputPath);
        FileOutputFormat.setOutputPath(job3, job3Output);

        job3.setMapperClass(HashtagMostUsedByUserMap.class);
        job3.setReducerClass(HashtagMostUsedByUserReduce.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        int job3Ok = job3.waitForCompletion(true) ? 0 : 1;

        if(job3Ok != 0) {
            return job3Ok;
        }

        /**
         * JOB 4 joins the outputs of job 2 and job 3
         */



        return 0;
    }

    public static class HashtagMostUsedByUserMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();
        private long numRecords = 0;

        private static final HashtagExtractor EXTRATOR = new HashtagExtractor();

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String user = "";
            String message = "";

            String line = lineText.toString();
            try {
                JSONObject obj = new JSONObject(line);
                user = obj.getString("user");
                message = obj.getString("message");
            } catch (JSONException e) {
                LOGGER.error("Can't parse JSON.", e);
            }

            Text currentWord;
            Text currentUser;
            for (String word : EXTRATOR.extract(message)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word);
                currentUser = new Text(user);
                context.write(currentWord, currentUser);
            }
        }
    }

    public static class HashtagMostUsedByUserReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text word, Iterable<Text> users, Context context)
                throws IOException, InterruptedException {

            String mostUser = getMostUser(users);
            if(mostUser != null) {
                mostUser = mostUser.replace("http://twitter.com/", "");

                context.write(word, new Text(mostUser));
            }

        }

        public static String getMostUser(Iterable<Text> users) {

            Map<String, Integer> userCountMap = new HashMap<String, Integer>();
            for(Text user : users) {
                String userStr = user.toString();
                int val = 0;
                if(userCountMap.containsKey(userStr)) {
                    val = userCountMap.get(userStr);
                }
                userCountMap.put(userStr, val+1);
            }

            int most = 0;
            String user = null;

            for(String currentUser : userCountMap.keySet()) {
                if(userCountMap.get(currentUser) > most) {
                    most = userCountMap.get(currentUser);
                    user = currentUser;
                }
            }

            return user;
        }
    }

}
