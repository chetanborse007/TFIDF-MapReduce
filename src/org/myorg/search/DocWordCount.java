/******************************************************************************//*!
* @File          DocWordCount.java
* 
* @Title         MapReduce application for finding total occurrences of every 
* 				 unique word in a document.
* 
* @Author        Chetan Borse
* 
* @EMail         cborse@uncc.edu
* 
* @Created on    09/29/2016
* 
*//*******************************************************************************/ 


package org.myorg.search;


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/******************************************************************************
* @Class		DocWordCount
* @Description	Class representing MapReduce implementation for word count in 
* 				every document.
* 				This class implements static 'Map' and 'Reduce' classes. It 
* 				also holds job configuration for 'DocumentWordCount'.
******************************************************************************/
public class DocWordCount extends Configured implements Tool {

	/******************************************************************************
	* @StaticClass	Map
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<Word>&#&#&<Filename>, <Count>’.
	******************************************************************************/
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE 	   = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		/* Overridden map method */
		@Override
  		public void map(LongWritable offset, Text lineText, Context context)
  							throws IOException, InterruptedException {
			String line;
			String fileName;
			String currentTerm;

			// 1. Convert to lower case so that same word written in diffrent cases can 
			// be mapped to the same key.
			// 2. Remove tabs with spaces, otherwise it can be conflict with tab 
			// separator that separates output key-value pair.
			line = lineText.toString().toLowerCase();
			line = line.replaceAll("\\t", " ");

			// Get file name using meta information of input split.
			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			// Split every line into tokens.
			// Create a new key and write (new key, 1) to context object.
			for (String term : WORD_BOUNDARY.split(line)) {
				if (term.trim().isEmpty()) {
					continue;
				}
				currentTerm = term.trim() + "&#&#&" + fileName;
				context.write(new Text(currentTerm), ONE);
			}
		}
	}

	/******************************************************************************
	* @StaticClass	Reduce
	* @Description	Reduce class for aggregating word counts for a same key.
	******************************************************************************/
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		/* Overridden reduce method */
		@Override 
		public void reduce(Text term, Iterable<IntWritable> counts, Context context)
								throws IOException, InterruptedException {
			int totalCount = 0;

			// Aggregate word counts for a same key.
			for (IntWritable count : counts) {
				totalCount += count.get();
			}

			context.write(term, new IntWritable(totalCount));
		}
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 2. It consists 
	* 									input and output paths.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(this.getClass());
		job.setJobName(" DocumentWordCount ");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	/* Entry point */
	public static void main(String[] args) throws Exception {
		String input;
		String output;
		int status;
		
		// Read input and output paths from user.
		input  = args[0];
		output = args[1];
		
		// Call driver function.
		status = ToolRunner.run(new DocWordCount(), new String[] {input, output});
		
		System.exit(status);
	}

}

