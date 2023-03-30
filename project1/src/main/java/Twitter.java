import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {

    public static class FirstMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int id = s.nextInt();
            int follower_id = s.nextInt();
            context.write(new IntWritable(follower_id),new IntWritable(id));
            s.close();
        }
    }

    public static class FirstReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable follower_id, Iterable<IntWritable> ids, Context context )
                           throws IOException, InterruptedException {
           int count = 0;
            for (IntWritable i: ids) {
                
                count=count+1;
            };
            context.write(follower_id,new IntWritable(count));
        }
    }

    public static class SecondMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            
            int follower_id = s.nextInt();
            int count = s.nextInt();
            context.write(new IntWritable(count),new IntWritable(1));
            s.close();
        }
    }

    public static class SecondReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable count, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
           int sum = 0;
            for (IntWritable i: values) {
                
                sum = sum +i.get();
            };
            context.write(count,new IntWritable(sum));
        }
    }


    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Map_Reduce1");
        job.setJarByClass(Twitter.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

        Job job1 = Job.getInstance();
        job1.setJobName("Map_Reduce1");
        job1.setJarByClass(Twitter.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(SecondMapper.class);
        job1.setReducerClass(SecondReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[1]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
    }
}
