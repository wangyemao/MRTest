package com.baina.groot;
/**
 * Created by Administrator on 2016/4/24.
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class UserRatingCount {

    // userRatingCount mapper
    public static class UserRatingCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text UserKey = new Text();
        public void map(LongWritable key,Text value,Context context)
                throws IOException, InterruptedException {
            String[] items = value.toString().split("::");
            System.out.println("--------MovieId ID :-----------"+items[1]);
            UserKey.set(items[1]);
            context.write(UserKey,one);
        }
    }

    //userRatingCount reducer
    public static class UserRatingCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values,Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum +=val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"UserRatingCountJob");
        job.setJarByClass(UserRatingCount.class);
        job.setMapperClass(UserRatingCountMapper.class);
        job.setReducerClass(UserRatingCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,output);
        System.exit(job.waitForCompletion(true)?0:1);
    }



}
