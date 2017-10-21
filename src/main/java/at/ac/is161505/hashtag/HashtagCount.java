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

/**
 * Created by n17405180 on 21.10.17.
 */
public class HashtagCount extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagCount.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HashtagCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "hashtagcount");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(HashtagCount.Map.class);
        job.setReducerClass(HashtagCount.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private long numRecords = 0;

        private static final HashtagExtractor EXTRATOR = new HashtagExtractor();

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String time;
            String user = "";
            String message = "";

            String line = lineText.toString();
            try {
                JSONObject obj = new JSONObject(line);
                time = obj.getString("time");
                user = obj.getString("user");
                message = obj.getString("message");
            } catch (JSONException e) {
                LOGGER.error("Can't parse JSON.", e);
            }

            Text currentWord = new Text();
            for (String word : EXTRATOR.extract(message)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word);
                context.write(currentWord, one);
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

}
