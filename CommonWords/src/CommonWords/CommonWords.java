package CommonWords;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;

public class CommonWords {
//	static TreeMap<String, Integer> sortMap = new TreeMap<String, Integer>();
	
	// Mapper1
	public static class TokenizerMapper1 extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private final static Text one = new Text("1_s1");
		Set<String> stopwords = new HashSet<String>();
		String regEx = "[^A-Za-z0-9']";
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				Path path = new Path("stopwords.txt");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String word = null;
				while((word = br.readLine()) != null) {
					stopwords.add(word);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
            while (itr.hasMoreTokens()) {
            	String tmp_itr = itr.nextToken();                
                if (stopwords.contains(tmp_itr)) {                	
                	continue;
                }             
                else {
                	tmp_itr = tmp_itr.replaceAll(regEx, " ").trim();             	
                }
                if (tmp_itr.contains(" ")){
                	String list_itr[] = tmp_itr.split(" ");
                	for (String item : list_itr) {                		
                		if (stopwords.contains(item)) {
                			continue;
                        }
                		else if (item.equals(" ")) {
                			continue;
                		}           
                		else if (item.equals("")) {
                			continue;
                		}
                		else if (StringUtils.isBlank(item)) {
                			continue;
                		}
                		else {
                			word.set(item);
                			context.write(word, one);
                		}                		
                	}
                }else {
                	word.set(tmp_itr);
        			context.write(word, one);
            	} 
            }
		}
	}
	
	// Mapper2
	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private final static Text one = new Text("1_s2");
		Set<String> stopwords = new HashSet<String>();
		String regEx = "[^A-Za-z0-9']";
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				Path path = new Path("stopwords.txt");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String word = null;
				while((word = br.readLine()) != null) {
					stopwords.add(word);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
            while (itr.hasMoreTokens()) {
            	String tmp_itr = itr.nextToken();                
                if (stopwords.contains(tmp_itr)) {                	
                	continue;
                }             
                else {
                	tmp_itr = tmp_itr.replaceAll(regEx, " ").trim();             	
                }
                if (tmp_itr.contains(" ")){
                	String list_itr[] = tmp_itr.split(" ");
                	for (String item : list_itr) {                		
                		if (stopwords.contains(item)) {
                			continue;
                        }
                		else if (item.equals(" ")) {
                			continue;
                		}
                		else if (item.equals("")) {
                			continue;
                		}
                		else if (StringUtils.isBlank(item)) {
                			continue;
                		}
                		else {
                			word.set(item);
                			context.write(word, one);
                		}                		
                	}
                }else {
                	word.set(tmp_itr);
        			context.write(word, one);
            	} 
            }
		}
	}
	
	//Reducer1
	public static class Reducer1 extends Reducer<Text, Text, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			int sum = 0, sum1 = 0, sum2 = 0;
			int val_int = 0;
            for (Text val : values) {
            	String s[] = val.toString().split("_");
                val_int = Integer.parseInt(s[0]);
                if (s[1].equals("s1")) {
                	sum1 += val_int;
                }
                if (s[1].equals("s2")) {
                	sum2 += val_int;
                }
            }
            if (sum1 > sum2) {
            	sum = sum2;
            }
            else {
            	sum = sum1;
            }
            if (sum != 0) {
	            result.set(sum);
	            context.write(key, result);
            }
		}
	}

	//reverseMapper   key:value --> value:key
	public static class reverseMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			String val = value.toString().replaceAll("\t", "-");
			String tmp_key = val.split("-")[0];
			String tmp_value = val.split("-")[1];
        	context.write(new IntWritable(Integer.parseInt(tmp_value)), new Text(tmp_key));
            }
	}
	
	//reverseReducer value:key --> key:value
	public static class reverseReducer extends Reducer<IntWritable, Text, Text, IntWritable>{		
		int count = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		    count = 0;
		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			if (count < 10) {
				for (Text val : values) {
					context.write(val, key);
					count ++;
					if (count >= 10) {
						break;
					}
				}
			}
		}
	}
	
	//comparator --> descending sort
	public static class IntKeyDescComparator extends WritableComparator{
		protected IntKeyDescComparator() {
			super(IntWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
	}
	
///////////////////////////////////////////////////////////////////////////////////
//main function

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		//here you can change another way to get the input and output
		
		if (otherArgs.length != 3) {
		System.err
		.println("Usage: wordcountmultipleinputs <input1> <input2> <out>");
		System.exit(2);
		}
		
		Job job = new Job(conf, "word count multiple inputs");
		job.setJarByClass(CommonWords.class);
		Path tmp_path = new Path("tempDir");
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, TokenizerMapper1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, TokenizerMapper2.class);
		job.setReducerClass(Reducer1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
			
		FileOutputFormat.setOutputPath(job, tmp_path);
		
		if (job.waitForCompletion(true)) {
			Job sortJob = new Job(conf, "sort");
			sortJob.setJarByClass(CommonWords.class);
			
			sortJob.setMapperClass(reverseMapper.class);
			sortJob.setReducerClass(reverseReducer.class);
			sortJob.setSortComparatorClass(IntKeyDescComparator.class);
			
			sortJob.setMapOutputKeyClass(IntWritable.class);
			sortJob.setMapOutputValueClass(Text.class);
			sortJob.setOutputKeyClass(Text.class);
			sortJob.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(sortJob, tmp_path);
			FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[2]));
			System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
		}	
	}
}
