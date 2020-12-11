package com.lyyq;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Prediction {
    public class WordCountMapper 
            extends Mapper<LongWritable,Text,Text,IntWritable>{
        Text item = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if(fields[6].equals("1")||fields[6].equals("2")||fields[6].equals("3")){
                item.set(fields[1]);
                context.write(item, v);
            } 
        }
    }


    public class WordCountReducer 
            extends Reducer<Text,IntWritable,Text,IntWritable>{
        IntWritable v = new IntWritable();
        Map<String,Integer> map=new HashMap<String,Integer>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable value : values) {
                sum+=value.get();
            }
 
            String k=key.toString();
            map.put(k, sum);
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String,Integer>> list=new LinkedList<Map.Entry<String,Integer>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (int)(o2.getValue()-o1.getValue());
                }
            });
            
            for(int i=0;i<5;i++){
                context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
            }
        }    
    }

    public static void main(String[] args) 
        throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
            if (remainingArgs.length !=2) {
                System.err.println("Usage: wordcount <in> <out> ");
                System.exit(2);
        } 

        Job job = Job.getInstance(conf);
        job.setJarByClass(Prediction.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outPath=new Path(args[1]);
        FileSystem fstm = FileSystem.get(conf);
        if (fstm.exists(outPath)) {
            fstm.delete(outPath, true);
        }
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
