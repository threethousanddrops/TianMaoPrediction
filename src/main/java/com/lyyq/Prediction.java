package com.lyyq;             

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.*; 
import java.net.URI; 
import java.io.BufferedReader;
import java.io.FileReader; 

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;



import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.StringUtils;  


import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.io.WritableComparable;   
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;   
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;   

import org.apache.hadoop.io.NullWritable;   


public class Prediction
{


    public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable>{
		
		
        private final static IntWritable one  = new IntWritable(1);
        //private Text word = new Text();
		private Text item = new Text();
		//private Text timestamp = new Text();
		//private Text eleven = new Text();
		
		
		//2.0
		private Configuration conf;   //定义conf
		private boolean caseSensitive;    //定义caseSensitive
		
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration( );
			caseSensitive = conf.getBoolean ( "wordcount.case.sensitive",true);
			if (conf.getBoolean ( "wordcount.skip.patterns" , true)){
				URI[ ] patternsURIs = Job.getInstance(conf).getCacheFiles();
				for (URI patternsURI: patternsURIs){
					Path patternsPath = new Path(patternsURI.getPath());
					String patternsFileName = patternsPath.getName( ).toString();
					parseSkipFile(patternsFileName);
				}
			}
		}

		//2.0
		private BufferedReader fis;     //定义fis
		private Set<String> patternsToSkip = new HashSet<String>();  //定义patternsToSkip
		
		private void parseSkipFile(String fileName){
			try{
				fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while((pattern = fis.readLine())!=null){
					patternsToSkip.add(pattern);
				}
			}catch(IOException ioe){
				System.err.println("Caught exception while parsing the cached file " + StringUtils.stringifyException(ioe));
			}
		}
		
		
		
		
        //map
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            
			String line = value.toString();
			String[] datas = line.split(",");
			
			boolean ifExist = false;
			
			for(String pattern:patternsToSkip){             //2.0 消去停词
			
				pattern = pattern.replaceAll(",","");
				if(datas[0].equals(pattern)){
					ifExist = true;
				}
			
            }  
			
			if(ifExist == false){
				if(datas[5].equals("1111")){
					if(datas[6].equals("1") || datas[6].equals("2") || datas[6].equals("3")){
						item.set(datas[3]);
						context.write(item, one);
					}
				} 
			}
			
			
        }
    }

    public static class IntSumReducer extends Reducer <Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
        //reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val:values){                    //遍历迭代器values 得到同一key的所有value
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
	
	private static class Decrease extends IntWritable.Comparator {
		public int compare(WritableComparable value1, WritableComparable value2) {
			int t1 = -super.compare(value1, value2);
			return t1;
		}
		public int compare(byte[] key1, int value1, int value12, byte[] key2, int value2, int value22) {
			int t2 = -super.compare(key1, value1, value12, key2, value2, value22);
			return t2;
		}
	}
	
	public static class ElevenReducer extends Reducer <IntWritable,Text,Text,IntWritable>{
		private int OutputSum=0;
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val:values)
			{
				OutputSum = OutputSum+1;
				if(OutputSum > 100)             //输出前（）个
					return;
				else{
					String t = OutputSum + ":" + val.toString() + ",";
					Text WORD = new Text(t);
					context.write(WORD, key);
				}
			}
		}
	}

    public static void main(String[] args) throws Exception{
		
        Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf,args);  
        //命令行参数
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //if (otherArgs.length!=2){
            //System.err.println("Usage:wordcount <in><out>");
            //System.exit(2);
        //}
        String[] remainingArgs = optionParser.getRemainingArgs();         
		
		//Path TempPath = new Path("TempOutput11");           
		Path TempPath = new Path("ShakespeareTempOutput");        
		
		if(!(remainingArgs.length!=2 || remainingArgs.length != 4)){                   
            System.err.println("Usage:wordcount <in> <out> [-skip skipPatternFile]");      
            System.exit(2);    
        }      
		
        @SuppressWarnings("deprecation")             
        Job job = new Job(conf,"count");
        job.setJarByClass(task12.class);         
        job.setMapperClass(TokenizerMapper.class);            //Mapper
        job.setCombinerClass(IntSumReducer.class);            //Combine
        job.setReducerClass(IntSumReducer.class);             //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);                     //设置job输出的value
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);     
	
	
		List<String> otherArgs = new ArrayList<String>();                 
        for(int i=0;i< remainingArgs.length;++i){                            
            if("-skip".equals(remainingArgs[i])){
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns",true);
            }else{
                otherArgs.add(remainingArgs[i]);
            }
        }

        //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));           //输入文件路径
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));           //2.0
        
		FileOutputFormat.setOutputPath(job,TempPath);       
		
		job.waitForCompletion(true);                        

		Job job2 = new Job(conf, "sort");       
		job2.setJarByClass(task12.class);         //                
		FileInputFormat.addInputPath(job2, TempPath);                   
		job2.setInputFormatClass(SequenceFileInputFormat.class);       
		job2.setMapperClass(InverseMapper.class);                     
		//job2.setNumReduceTasks(1);                                  
		job2.setReducerClass(ElevenReducer.class);     
			
		
	
		//FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));      
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(1))); 
		
		
		job2.setOutputKeyClass(IntWritable.class);          
		job2.setOutputValueClass(Text.class);                     
		job2.setSortComparatorClass(Decrease.class);  
 
		job2.waitForCompletion(true);            
		
		FileSystem.get(conf).delete(TempPath);   

        System.exit(job.waitForCompletion(true)?0:1);
    }

}
