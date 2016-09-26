import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;


public class SimilarWordsTool 
{
	private static final String INTER_OUTPUT_PATH = "inter_output_path";
	 
	static int threshold;
	static int flag = 1;

	public static class FirstMapper extends Mapper<Object, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String rowID = itr.nextToken();
			List<String> listOfElement = new ArrayList<String>();
			while (itr.hasMoreTokens()) 
			{
				listOfElement.add(itr.nextToken());	   
			}
			Set<String> hashOfList = new HashSet<String>();
			hashOfList.addAll(listOfElement);
			listOfElement.clear();
			listOfElement.addAll(hashOfList); 
			List<String> tupleOfList = new ArrayList<String>();
			for(int i = 0 ; i < listOfElement.size()-1;i++)
			{
				for(int j = i+1 ; j < listOfElement.size();j++)
				{
					String tuple1 = listOfElement.get(i);
					String tuple2 = listOfElement.get(j);
					String resultTuple;
					if(tuple1.compareTo(tuple2) < 0)
					{
						resultTuple = "("+tuple1+","+tuple2+")";
					}
					else
					{
						resultTuple = "("+tuple2+","+tuple1+")";
					}
					tupleOfList.add(resultTuple);
				}
			}
			for(int i = 0 ; i < tupleOfList.size() ; i++)
			{
				word.set(tupleOfList.get(i));
				context.write(word, one);
			}
		}
	}
	
	public static class SecondMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String numNotUse = itr.nextToken(); // just to reach next element
			while (itr.hasMoreTokens()) 
			{ 
				word.set(itr.nextToken());
				context.write(word, one);  
			}
		}
	}

	public static class FirstReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context
		) throws IOException, InterruptedException 
		{
			int sumOfCombiner = 0;
			for (IntWritable tempVal : values) {
				int value = tempVal.get();
				sumOfCombiner += value;
			}		
			result.set(sumOfCombiner);	
			if(sumOfCombiner >= threshold || flag == 0)
            {
                //System.out.println("fag = 0 " + key);
                context.write(key, result);
            }

		}
	}
	
	public static class SecondReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context
		) throws IOException, InterruptedException 
		{
			int sumOfCombiner = 0;
			for (IntWritable tempVal : values) {
				int value = tempVal.get();
				sumOfCombiner += value;
			}			
			result.set(sumOfCombiner);
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception 
	{	
		Configuration confMR1 = new Configuration();
		FileSystem fs = FileSystem.get(confMR1);
		Job mapreduceJob1 = Job.getInstance(confMR1, "Map Reduce Job 1");
		mapreduceJob1.setJarByClass(SimilarWordsTool.class);
		mapreduceJob1.setMapperClass(FirstMapper.class); // map made -> <word,1> 
		mapreduceJob1.setCombinerClass(FirstReducer.class); // combine each map 
		mapreduceJob1.setReducerClass(FirstReducer.class);  // reduce different map results into one
		mapreduceJob1.setOutputKeyClass(Text.class);			// set the key class
		mapreduceJob1.setOutputValueClass(IntWritable.class); // set the value class
		if(args[0].equals("sim"))
		{
			fs.delete(new Path(args[3]), true);
			threshold = Integer.parseInt(args[1]);
			FileInputFormat.addInputPath(mapreduceJob1, new Path(args[2])); // order the path directory
			FileOutputFormat.setOutputPath(mapreduceJob1, new Path(args[3]));
			System.exit(mapreduceJob1.waitForCompletion(true) ? 0 : 1);
		}
		else
		{
			fs.delete(new Path(args[2]), true);
			
			flag = 0;
			FileInputFormat.addInputPath(mapreduceJob1, new Path(args[1])); // order the path directory
			FileOutputFormat.setOutputPath(mapreduceJob1, new Path(INTER_OUTPUT_PATH));
			
			mapreduceJob1.waitForCompletion(true);
			
			Configuration confMR2 = new Configuration();
			Job mapreduceJob2 = Job.getInstance(confMR2, "Map Reduce Job 2");	
			
			mapreduceJob2.setJarByClass(SimilarWordsTool.class);
			mapreduceJob2.setMapperClass(SecondMapper.class); // map made -> <word,1> 
			mapreduceJob2.setCombinerClass(SecondReducer.class); // combine each map 
			mapreduceJob2.setReducerClass(SecondReducer.class);  // reduce different map results into one
			mapreduceJob2.setOutputKeyClass(Text.class);			// set the key class
			mapreduceJob2.setOutputValueClass(IntWritable.class); // set the value class
			
			FileInputFormat.addInputPath(mapreduceJob2, new Path(INTER_OUTPUT_PATH)); // order the path directory
			FileOutputFormat.setOutputPath(mapreduceJob2, new Path(args[2]));
						
			
			
			if(mapreduceJob2.waitForCompletion(true))
			{
				fs.delete(new Path(INTER_OUTPUT_PATH), true);
				System.exit(0);
			}
			
			//System.exit(mapreduceJob2.waitForCompletion(true) ? 0 : 1);
		}			
	}
}
