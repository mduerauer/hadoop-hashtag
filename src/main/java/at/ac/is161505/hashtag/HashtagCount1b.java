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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by n17405180 on 21.10.17.
 */
public class HashtagCount1b extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagCount1b.class);

    private static final int TOP_N = 50;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HashtagCount1b(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path step1Path = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job1 = Job.getInstance(getConf(), "hashtagcount");
        job1.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, step1Path);

        job1.setMapperClass(HashtagCount1a.Map.class);
        job1.setReducerClass(HashtagCount1a.Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        int job1Ok = job1.waitForCompletion(true) ? 0 : 1;

        if(job1Ok != 0) {
            return job1Ok;
        }

        Job job2 = Job.getInstance(getConf(), "hashtagcount_n");
        job2.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job2, step1Path);
        FileOutputFormat.setOutputPath(job2, outputPath);

        job2.setMapperClass(HashtagCount1b.TopNMap.class);
        job2.setReducerClass(HashtagCount1b.TopNReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        return job2.waitForCompletion(true) ? 0 : 1;

    }

    public static class TopNMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Map<String, Integer> countMap = new HashMap<String, Integer>();

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

            Map<String, Integer> sortedMap = MapUtils.sortByValueDesc(countMap);

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

        private Map<String, Integer> countMap = new HashMap<String, Integer>();

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

            Map<String, Integer> sortedMap = MapUtils.sortByValueDesc(countMap);

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
