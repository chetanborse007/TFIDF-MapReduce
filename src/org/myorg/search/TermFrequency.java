/******************************************************************************//*!
* @File          TermFrequency.java
* 
* @Title         MapReduce application for computing the logarithmic Term Frequency 
* 				 of every unique term in a document.
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
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


/******************************************************************************
* @Class		TermFrequency
* @Description	Class representing MapReduce implementation for computing the
* 				logarithmic Term Frequency of every unique term in a document.
* 				This class implements static 'TFMap' and 'TFReducer' classes. It 
* 				also holds job configuration for 'TermFrequency'.
******************************************************************************/
public class TermFrequency extends Configured implements Tool {

	/******************************************************************************
	* @StaticClass	TFMap
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<Word>&#&#&<Filename>, <Count>’.
	******************************************************************************/
	public static class TFMap extends Mapper<LongWritable, Text, Text, IntWritable> {
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
	* @StaticClass	TFReducer
	* @Description	Reduce class for aggregating word counts for a same key and 
	* 				for computing the logarithm Term Frequency.
	******************************************************************************/
	public static class TFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		/* Overridden reduce method */
		@Override 
		public void reduce(Text term, Iterable<IntWritable> counts, Context context)
								throws IOException, InterruptedException {
			double termFrequency = 0;

			// Aggregate word counts for a same key.
			for (IntWritable count : counts) {
				termFrequency += (double) count.get();
			}

			// Compute the logarithmic Term Frequency
			if (termFrequency > 0) {
				termFrequency = 1 + Math.log10(termFrequency);
			} else {
				termFrequency = 0;
			}

			context.write(term, new DoubleWritable(termFrequency));
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
		job.setJobName(" TermFrequency ");
		job.setMapperClass(TFMap.class);
		job.setReducerClass(TFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Delete intermediate output directory of Term Frequencies, if it exists.
		FileSystem fs = FileSystem.get(getConf());
		Path TFPath   = new Path(args[1]);
		fs.delete(TFPath);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, TFPath);
		
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
		status = ToolRunner.run(new TermFrequency(), new String[] {input, output});
		
		System.exit(status);
	}

}

