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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HashtagCount1c(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "hashtagtouser");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(HashtagCount1c.UserMap.class);
        job.setReducerClass(HashtagCount1c.UserReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class UserMap extends Mapper<LongWritable, Text, Text, Text> {

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

    public static class UserReduce extends Reducer<Text, Text, Text, Text> {

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
