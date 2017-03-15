/******************************************************************************//*!
* @File          Rank.java
* 
* @Title         MapReduce application for ranking the search hits in descending 
* 				 order by their accumulated score.
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


/******************************************************************************
* @Class		Rank
* @Description	Class representing MapReduce implementation for ranking 
* 				the search hits in descending order by their accumulated score.
* 				This class implements static 'RankMap' and 'RankReducer' 
* 				classes. It also holds job configuration for 'Ranker'.
******************************************************************************/
public class Rank extends Configured implements Tool {

	/******************************************************************************
	* @StaticClass	RankMap
	* @Description	This map class flips the key-value pair.
	* 				It outputs value as a key and key as a value such as,
	* 				Input key-value pair: ‘<Filename>, <TFIDF Score>’ and 
	* 				Output key-value pair: ‘<TFIDF Score>, <Filename>’.
	******************************************************************************/
	public static class RankMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
							throws IOException, InterruptedException {
			String doc;
			double score;

			doc   = lineText.toString().split("\t")[0];
			score = Double.parseDouble(lineText.toString().split("\t")[1]);
			
			context.write(new DoubleWritable(score), new Text(doc));
		}
	}

	/******************************************************************************
	* @StaticClass	RankReducer
	* @Description	Reduce class takes output key-value pairs from map class. It
	* 				flips them and outputs without any significant transformation.
	******************************************************************************/
	public static class RankReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		/* Overridden reduce method */
		@Override 
		public void reduce(DoubleWritable score, Iterable<Text> docs, Context context)
								throws IOException, InterruptedException {
			for (Text doc : docs) {
				context.write(doc, score);
			}
		}
	}
	
	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 2. It consists 
	* 									input path for unordered search results
	* 									and output path for search hits ranked 
	* 									by document scores.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(this.getClass());
		job.setJobName(" Ranker ");
		job.setMapperClass(RankMap.class);
		job.setReducerClass(RankReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	/* Entry point */
	public static void main(String[] args) throws Exception {
		String input;
		String output;
		int RankStatus;
		
		// Read from user input path for unordered search results and 
		// output path for search hits ranked by document scores.
		input  = args[0];
		output = args[1];
		
		// Call driver function.
		RankStatus = ToolRunner.run(new Rank(), new String[] {input, output});

		System.exit(RankStatus);
	}

}

