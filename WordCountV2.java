import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.io.*;

public class WordCountV2  extends Configured implements Tool{

    /**
     * Main function which calls the run method and passes the args using ToolRunner
     * @param args Two arguments input and output file paths
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountV2(), args);
        System.exit(exitCode);
    }

    /**
     * Run method which schedules the Hadoop Job
     * @param args Arguments passed in main function
     */
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments <input> <output> files\n",
                    getClass().getSimpleName());
            System.err.println(String.format("%s %s",args[0].toString(),args[1].toString()));
            return -1;
        }

        //Initialize the Hadoop job and set the jar as well as the name of the Job
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCountV2.class);
        job.setJobName("WordCountV2");

        NYUZInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(NYUZInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the MapClass and ReduceClass in the job
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //Wait for the job to complete and print if the job was successful or not
        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }

    public static class MyMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Text key, BytesWritable value, Context context)throws IOException,InterruptedException {
            String result = new String(value.getBytes());
            String[] lines = result.split("\\r?\\n");
            String regEx1 = "[^a-zA-Z0-9]+";  // 只允许字母和数字
            String regEx2 = "[a-zA-Z0-9]+";
            String firstBigrams;
            String secondBigrams;
            for (String line:lines){
                String[] results = line.split(regEx1);
                if (results.length > 1){
                    int i = 0;
                    Pattern pattern = Pattern.compile(regEx2);
                    Matcher matcher = pattern.matcher(results[0]);
                    boolean rs = matcher.matches();
                    if (rs == true){
                        firstBigrams = results[i];
                        i += 1;
                        while (i < results.length){
                            secondBigrams = results[i];
                            word.set(firstBigrams.toLowerCase() + " " + secondBigrams.toLowerCase());
                            context.write(word, one);
                            i += 1;
                            firstBigrams = secondBigrams;
                        }
                    }
                }
            }
        }

    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
