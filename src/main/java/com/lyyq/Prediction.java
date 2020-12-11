package com.lyyq;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Popular {

public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Set<String> patternsToskip=new HashSet<>();
    protected void setup(Context context) throws IOException, InterruptedException{
        FileSystem fs = FileSystem.get(context.getConfiguration());

        Path path1 = new Path("hdfs://lyyq181850099-master:9000/task/user_info_skip.csv");
        BufferedReader reader1 = new BufferedReader(new InputStreamReader(fs.open(path1)));
        String liner;
        while ((liner = reader1.readLine()) != null) {
          patternsToskip.add(liner.toLowerCase());
        }
        reader1.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if(fields[5].equals("1111")){
            if(!(this.patternsToskip.contains(fields[0]))){
                if(fields[6].equals("1")||fields[6].equals("2")||fields[6].equals("3")){
                this.word.set(fields[3]);
                context.write(this.word, new Text("1"));
                }
            }
        }
    }
}

public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
   
    private IntWritable result = new IntWritable();
    private Text word = new Text();

    private static TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
        @Override
        public int compare(Integer x, Integer y) {
            return y.compareTo(x);
        }
    });

    public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException{
          int sum = 0;
          for (Text val: values)
          {
              sum+=Integer.valueOf(val.toString());
          }
          treeMap.put(new Integer(sum), key.toString());
      }

    protected void cleanup(Context context)
        throws IOException,InterruptedException{
        Set<Map.Entry<Integer, String>> set = treeMap.entrySet();
        int count =1;
        for (Map.Entry<Integer, String> entry : set) {
            this.result.set(entry.getKey());
            this.word.set(count+":"+entry.getValue()+", "+entry.getKey());
            context.write(word, new Text(""));
            count++;
            if(count>100){
              return;
            }
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (remainingArgs.length !=2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    } 

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Popular.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    Path outPath=new Path(args[1]);
    FileSystem fstm = FileSystem.get(conf);
    if (fstm.exists(outPath)) {
        fstm.delete(outPath, true);
    }

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, outPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
