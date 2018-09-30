import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IdfCountJob extends Configured implements Tool {

    public static class IdfCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Boolean> flags = new HashMap<>();

            String line = value.toString().toLowerCase();

            String fixedRegex = "[\\p{L}" + String.valueOf(((char) 775)) + "]+";

            Matcher m = Pattern.compile(fixedRegex).matcher(line);

            while (m.find()) {
                String word = m.group();

                if (flags.containsKey(word)) {
                    continue;
                }

                context.write(new Text(word), one);
                flags.put(word, true);
            }
        }
    }

    public static class IdfCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> nums, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable i: nums) {
                sum += i.get();
            }

            context.write(word, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        if (System.getProperty("mapreduce.input.indexedgz.bytespermap") != null) {
            throw new Exception("Property = " + System.getProperty("mapreduce.input.indexedgz.bytespermap"));
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static Job GetJobConf(Configuration conf, final String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(IdfCountJob.class);
        job.setJobName(IdfCountJob.class.getCanonicalName());

        job.setInputFormatClass(DeflatePageInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        FileSystem fs = new Path("/").getFileSystem(conf);
        FileStatus[] list = fs.listStatus(new Path("/"), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                Matcher m = Pattern.compile(input).matcher(path.toString());
                return m.matches();
            }
        });

        for (FileStatus f: list) {
            FileInputFormat.addInputPath(job, f.getPath());
        }

        FileInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(IdfCountMapper.class);
        job.setCombinerClass(IdfCountReducer.class);
        job.setReducerClass(IdfCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new IdfCountJob(), args);
        System.exit(exitCode);
    }
}
