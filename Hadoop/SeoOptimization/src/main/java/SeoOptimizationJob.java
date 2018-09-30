import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SeoOptimizationJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SeoOptimizationJob(), args);
        System.exit(rc);
    }

    public static class SeoMapper extends Mapper<LongWritable, Text, SeoPair, IntWritable> {
        private final Logger logger = LoggerFactory.getLogger(SeoMapper.class);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] kv = line.split("\t");

            if (kv.length != 2) {
                context.getCounter("COMMON_COUNTERS", "SkippedValues").increment(1);

                return;
            }

            String host = null;
            try {
                host = new URI(kv[1]).getHost();
            } catch (URISyntaxException e) {
                context.getCounter("COMMON_COUNTERS", "SkippedValues").increment(1);

                return;
            }

//            Yet Another Host Extraction Method
//            String[] splits = kv[1].split("://");
//            String host = null;
//
//            if (splits.length == 2) {
//                host = splits[1];
//            } else if (splits.length == 1 && !kv[1].contains("://")) {
//                host = splits[0];
//            } else {
//                context.getCounter("COMMON_COUNTERS", "SkippedValues").increment(1);
//
//                return;
//            }
//
//            if (host.startsWith("www.")) {
//                host = host.substring(4);
//            }
//
//            int idx = host.indexOf('/');
//            if (idx != -1) {
//                host = host.substring(0, idx);
//            }
//
//            idx = host.indexOf(':');
//            if (idx != -1) {
//                host = host.substring(0, idx);
//            }
//
            if (host == null) {
                context.getCounter("COMMON_COUNTERS", "SkippedValues").increment(1);

                return;
            }

            context.write(new SeoPair(host, kv[0]), new IntWritable(1));
        }
    }

    public static class SeoReducer extends Reducer<SeoPair, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(SeoPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String url = key.getUrl().toString();

            String curQuery = "";
            String bestQuery = "";
            int bestCnt = 0;
            int curCnt = 0;

            for (IntWritable p : values) {
                String query = key.getQuery().toString();

                if (!query.equals(curQuery)) {
                    context.getCounter("COMMON_COUNTERS", "UNIQUE_QUERIES").increment(1);

                    curQuery = query;
                    curCnt = 0;
                }

                curCnt++;

                if (curCnt > bestCnt || (curCnt == bestCnt && curQuery.compareTo(bestQuery) < 0)) {
                    bestCnt = curCnt;
                    bestQuery = curQuery;
                }
            }

            if (bestCnt >= context.getConfiguration().getLong(MIN_CLICKS, 1)) {
                String outKey = String.format("%s\t%s", url, bestQuery);
                context.write(new Text(outKey), new IntWritable(bestCnt));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            super.cleanup(context);
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(SeoPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SeoPair) a).compareTo((SeoPair) b);
        }
    }

    public static class SeoPartitioner extends Partitioner<SeoPair, IntWritable> {
        @Override
        public int getPartition(SeoPair key, IntWritable value, int numPartitions) {
            return Math.abs(key.getUrl().hashCode()) % numPartitions;
        }
    }

    public static class SeoGrouper extends WritableComparator {
        protected SeoGrouper() {
            super(SeoPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text s1 = ((SeoPair) a).getUrl();
            Text s2 = ((SeoPair) b).getUrl();

            return s1.compareTo(s2);
        }
    }

    public static Job GetJobConf(Configuration conf, final String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SeoOptimizationJob.class);
        job.setJobName(SeoOptimizationJob.class.getCanonicalName());

        FileOutputFormat.setOutputPath(job, new Path(out_dir));
        FileInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(SeoMapper.class);
//        job.setCombinerClass(SeoReducer.class);
        job.setReducerClass(SeoReducer.class);

        job.setPartitionerClass(SeoPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(SeoGrouper.class);

        job.setMapOutputKeyClass(SeoPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String MIN_CLICKS = "seo.minclicks";
}
