

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TokenPerNumberFoundDataSet {
    static HashMap<String, HashSet> numberOfInputPerToken =new HashMap<String, HashSet>();
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        String filename;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Text value_without_space= new Text(value.toString().trim());
            context.write(value_without_space,new Text(filename));

        }

        @Override
        public void setup(Context context) {

            org.apache.hadoop.mapreduce.lib.input.FileSplit  fileSplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit();
            filename = fileSplit.getPath().getName();
            System.out.println("filename>>>>>>>>>>>>>>>>>>>>>>>>> "+filename);


        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable value = new IntWritable(0);
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Text text=null;
            String  s="";
            HashSet <String> inputNames= new HashSet<String>();
            for (Text value2 : values) {
                inputNames.add(value2.toString());

            }
            value.set(sum);
            context.write(key, new IntWritable(inputNames.size()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(DataSetPerNumberOfTokens.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat .setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if (success) {
            
            FileSystem fs = FileSystem.get(conf);
            fs.copyToLocalFile(new Path(args[1]),new Path("D:\\big-data\\jobs_results"));
        }
        System.out.println(success);
    }



}
