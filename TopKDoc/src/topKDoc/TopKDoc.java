package topKDoc;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

public class TopKDoc {
	////////////////////// Statge1 ///////////////////////////
	// Mapper1 - compute frequency of every word in a document
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		Set<String> stopwords = new HashSet<String>();
		String regEx = "[^A-Za-z0-9']";
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				Path path = new Path("stopword/stopwords.txt");
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
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
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
                			word.set(item + "@d" + fileName.substring(4,5));
                			context.write(word, one);
                		}                		
                	}
                }else {
                	word.set(tmp_itr + "@d" + fileName.substring(4,5));
        			context.write(word, one);
            	} 
            }
		}		
		//Reducer1
		public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
			private IntWritable result = new IntWritable();
			
			public void reduce(Text key, Iterable<IntWritable> values, Context context) 
					throws IOException, InterruptedException {	 
	            int sum = 0;
	            for (IntWritable val : values) {
	                sum += val.get();
	            }
	            result.set(sum);
	            context.write(key, result);
	        }
		}
		
		////////////////////// Statge2 ///////////////////////////
		//Mapper2 - compute IF-IDF of every word w.r.t a document
		public static class Mapper2 extends Mapper<Object, Text, Text, Text> {	 
	        public void map(Object key, Text value, Context context)
	                throws IOException, InterruptedException {
	        	String val = value.toString().replaceAll("\t", "-");
				String tmp_key = val.split("-")[0].split("@")[0];
				String tmp_value = val.split("-")[0].split("@")[1] + "=" + val.split("-")[1];
				context.write(new Text(tmp_key), new Text(tmp_value));   
	        }
	    }
		
		//Reducer2
		public static class Reducer2 extends Reducer<Text, Text, Text, Text>{
			
			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {	
				int numOfDocWord = 0;
				// Need a ArrayList to store all these value. 
				//Visit value iteratively two times does,' work.Cannot go into the second iteration
				ArrayList<String> valList = new ArrayList<String>();
				for (Text val1 : values) {
					valList.add(val1.toString());
					numOfDocWord += 1;
				}
								
	            for (String val2 : valList) {
//	            	System.out.println(val2);
	            	int freq = Integer.parseInt(val2.split("=")[1]);
//	            	System.out.println(freq);
	            	double tfIdf = (1 + Math.log(freq)) * Math.log(8 / numOfDocWord);
//	            	System.out.println(tfIdf);
	            	String tmp_tfIdf = String.format("%.6f",tfIdf);
	            	String newKey = key.toString() + "@" + val2.split("=")[0];
	            	context.write(new Text(newKey), new Text(tmp_tfIdf));
	            }
	        }
		}
		
		////////////////////// Statge3 ///////////////////////////
		//Mapper3 - compute normalized TF-IDF of every word w.r.t a document
		public static class Mapper3 extends Mapper<Object, Text, Text, Text> {	 
	        public void map(Object key, Text value, Context context)
	                throws IOException, InterruptedException {
	        	String val = value.toString().replaceAll("\t", "-");
				String tmp_key = val.split("-")[0].split("@")[1];
				String tmp_value = val.split("-")[0].split("@")[0] + "=" + val.split("-")[1];
				context.write(new Text(tmp_key), new Text(tmp_value));   
	        }
	    }
		//Reducer3
		public static class Reducer3 extends Reducer<Text, Text, Text, Text>{
			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				double sum = 0;
				// Need a ArrayList to store all these value. 
				//Visit value iteratively two times does,' work.Cannot go into the second iteration
				ArrayList<String> valList = new ArrayList<String>();
				for (Text val1 : values) {
					valList.add(val1.toString());
					sum = sum + Math.pow(Double.parseDouble(val1.toString().split("=")[1]), 2);
				}								
	            for (String val2 : valList) {
	            	double normTfIdf = Double.parseDouble(val2.split("=")[1]) / Math.sqrt(sum);
	            	String tmp_normTfIdf = String.format("%.6f",normTfIdf);    	
	      	        String newKey = val2.split("=")[0] + "@" + key.toString();
	            	context.write(new Text(newKey), new Text(tmp_normTfIdf));
	            }
			}
		}
			
		////////////////////// Statge4 ///////////////////////////
		//Mapper4 - compute the relevance of every document w.r.t a query
		public static class Mapper4 extends Mapper<Object, Text, Text, Text>{			
			Set<String> querywords = new HashSet<String>();
			@Override
			protected void setup(Context context) {
				Configuration conf = context.getConfiguration();
				try {
					Path path = new Path("query/query.txt");
					FileSystem fs = FileSystem.get(new Configuration());
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
					String word = null;
					while((word = br.readLine()) != null) {
						querywords.add(word);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			public void map(Object key, Text value, Context context) 
					throws IOException, InterruptedException{
				String val = value.toString().replaceAll("\t", "-");
				String tmp_key = val.split("@")[1].substring(0,2);
				String tmp_word = val.split("@")[0];
				if (querywords.contains(tmp_word)) {
					String tmp_value = tmp_word + "=" + val.split("@")[1].substring(3);
					context.write(new Text(tmp_key), new Text(tmp_value));
				}
			}
		}
		//Reducer4
		public static class Reducer4 extends Reducer<Text, Text, Text, Text>{
			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				double sum = 0;
				// Need a ArrayList to store all these value. 
				//Visit value iteratively two times does,' work.Cannot go into the second iteration
				ArrayList<String> valList = new ArrayList<String>();
				for (Text val1 : values) {
					valList.add(val1.toString());
					sum = sum + Double.parseDouble(val1.toString().split("=")[1]);
				}
				String tmp_sum = String.format("%.6f",sum);
	            context.write(key, new Text(tmp_sum));
			}
		}
		
		////////////////////// Statge5 ///////////////////////////
		//sort documents w.r.t relevance
		//Mapper5 - reverse key : value
		public static class Mapper5 extends Mapper<Object, Text, DoubleWritable, Text> {	 
	        public void map(Object key, Text value, Context context)
	                throws IOException, InterruptedException {
	        	String val = value.toString().replaceAll("\t", "-");
				double tmp_key = Double.parseDouble(val.split("-")[1]);
				String tmp_value = val.split("-")[0];
				context.write(new DoubleWritable(tmp_key), new Text(tmp_value));   
	        }
	    }
		//Reducer5 - reverse the key : value pairs coming from Mapper5
		public static class Reducer5 extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
			int count = 0;
			@Override
			protected void setup(Context context) throws IOException, InterruptedException {
			    count = 0;
			}
			public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException{
				if (count < 4) {
					for (Text val : values) {
						String tmp_value = "file" + val.toString().substring(1) + ".txt";
						context.write(new Text(tmp_value), key);
						count ++;
					}
				}
			}
		}
		//comparator --> descending sort
		public static class DoubleKeyDescComparator extends WritableComparator{
			protected DoubleKeyDescComparator() {
				super(DoubleWritable.class, true);
			}			
			@Override
			public int compare(WritableComparable a, WritableComparable b) {
				return -super.compare(a, b);
			}
		}
		
		
		////////////////////////////////////////////////////////////////////////
		//main function
	    
	    public static void main(String[] args) throws Exception {
	        
	    	Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args)
	                .getRemainingArgs();
	        //here you can change another way to get the input and output
	        if (otherArgs.length != 2) {
	            System.err
	                    .println("Usage: wordcountmultipleinputs <input> <output>");
	            System.exit(2);
	        }
	        
	        //Stage1
	        Job job1 = new Job(conf, "word count");
	        job1.setJarByClass(TopKDoc.class);
	        Path tmp_path1 = new Path("tmp_out1");;
	 
	        job1.setMapperClass(Mapper1.class);
	        job1.setReducerClass(Reducer1.class);
	        job1.setMapOutputKeyClass(Text.class);
	        job1.setMapOutputValueClass(IntWritable.class);
	        job1.setOutputKeyClass(Text.class);
	        job1.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job1, new Path("input"));
	        FileOutputFormat.setOutputPath(job1, tmp_path1);
	        
	        //Stage2
	        if (job1.waitForCompletion(true)) {
		        Job job2 = new Job(conf, "calculate TF-IDF");
		        job2.setJarByClass(TopKDoc.class);
		        Path tmp_path2 = new Path("tmp_out2");;
		        job2.setMapperClass(Mapper2.class);
		        job2.setReducerClass(Reducer2.class);
		        job2.setMapOutputKeyClass(Text.class);
		        job2.setMapOutputValueClass(Text.class);
		        job2.setOutputKeyClass(Text.class);
		        job2.setOutputValueClass(Text.class);
		        FileInputFormat.addInputPath(job2, tmp_path1);
		        FileOutputFormat.setOutputPath(job2, tmp_path2);
		        
		        //Stage3
		        if (job2.waitForCompletion(true)) {			        
					Job job3 = new Job(conf, "calcelate normalized TF-IDF");
			        job3.setJarByClass(TopKDoc.class);
			        Path tmp_path3 = new Path("tmp_out3");;
			        job3.setMapperClass(Mapper3.class);
			        job3.setReducerClass(Reducer3.class);
			        job3.setMapOutputKeyClass(Text.class);
			        job3.setMapOutputValueClass(Text.class);
			        job3.setOutputKeyClass(Text.class);
			        job3.setOutputValueClass(Text.class);
			        FileInputFormat.addInputPath(job3, tmp_path2);
			        FileOutputFormat.setOutputPath(job3, tmp_path3);
			        
			        //Stage4
			        if (job3.waitForCompletion(true)) {			        
						Job job4 = new Job(conf, "calculate relevance");
				        job4.setJarByClass(TopKDoc.class);
				        Path tmp_path4 = new Path("tmp_out4");;
				        job4.setMapperClass(Mapper4.class);
				        job4.setReducerClass(Reducer4.class);
				        job4.setMapOutputKeyClass(Text.class);
				        job4.setMapOutputValueClass(Text.class);
				        job4.setOutputKeyClass(Text.class);
				        job4.setOutputValueClass(Text.class);
				        FileInputFormat.addInputPath(job4, tmp_path3);
				        FileOutputFormat.setOutputPath(job4, tmp_path4);
				        //Stage5
				        if (job4.waitForCompletion(true)) {			        
							Job job5 = new Job(conf, "sort by relevance");
					        job5.setJarByClass(TopKDoc.class);
					        job5.setMapperClass(Mapper5.class);
					        job5.setReducerClass(Reducer5.class);
					        job5.setSortComparatorClass(DoubleKeyDescComparator.class);
					        job5.setMapOutputKeyClass(DoubleWritable.class);
					        job5.setMapOutputValueClass(Text.class);
					        job5.setOutputKeyClass(DoubleWritable.class);
					        job5.setOutputValueClass(Text.class);
					        FileInputFormat.addInputPath(job5, tmp_path4);
					        FileOutputFormat.setOutputPath(job5, new Path("output"));
					        System.exit(job5.waitForCompletion(true) ? 0 : 1);
				        }				       
			        }
		        }
	        }
	    }
	}
}
