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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by n17405180 on 21.10.17.
 */
public class UserTweetCount2a extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserTweetCount2a.class);

    private static final int TOP_N = 50;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserTweetCount2a(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        String outputDir = args[1];

        Job job1 = Job.getInstance(getConf(), "usercount");
        Path job1OutputPath = new Path(outputDir + "/job1");
        job1.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, job1OutputPath);

        job1.setMapperClass(UserTweetCount2a.Map.class);
        job1.setReducerClass(UserTweetCount2a.Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        int job1Ok = job1.waitForCompletion(true) ? 0 : 1;

        if(job1Ok != 0) {
            return job1Ok;
        }

        Job job2 = Job.getInstance(getConf(), "usercount_n");
        Path job2OutputPath = new Path(outputDir + "/job2");
        job2.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job2, job1OutputPath);
        FileOutputFormat.setOutputPath(job2, job2OutputPath);

        job2.setMapperClass(UserTweetCount2a.TopNMap.class);
        job2.setReducerClass(UserTweetCount2a.TopNReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private long numRecords = 0;

        private static final HashtagExtractor EXTRATOR = new HashtagExtractor();

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String time = null;

            String line = lineText.toString();
            try {
                JSONObject obj = new JSONObject(line);
                time = obj.getString("time");
            } catch (JSONException e) {
                LOGGER.error("Can't parse JSON.", e);
            }

            if(time != null && !time.isEmpty()) {
                context.write(new Text(time.substring(0,10)), one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }

    public static class TopNMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private java.util.Map<String, Integer> countMap = new HashMap<String, Integer>();

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String[] splitted = lineText.toString().trim().split("\\s+");
            if(splitted.length == 2) {
                String hashtag = splitted[0];
                int count = Integer.parseInt(splitted[1]);
                countMap.put(hashtag, count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            java.util.Map<String, Integer> sortedMap = MapUtils.sortByValueDesc(countMap);

            int counter = 0;
            for (String key: sortedMap.keySet()) {

                if (counter ++ == TOP_N) {
                    break;
                }

                context.write(new Text(key), new IntWritable(sortedMap.get(key)));
            }

        }
    }

    public static class TopNReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private java.util.Map<String, Integer> countMap = new HashMap<String, Integer>();

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            countMap.put(word.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            java.util.Map<String, Integer> sortedMap = MapUtils.sortByValueDesc(countMap);

            int counter = 0;
            for (String key: sortedMap.keySet()) {

                if (counter ++ == TOP_N) {
                    break;
                }

                context.write(new Text(key), new IntWritable(sortedMap.get(key)));
            }
        }


    }

}
