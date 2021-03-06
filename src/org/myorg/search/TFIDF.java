/******************************************************************************//*!
* @File          TFIDF.java
* 
* @Title         MapReduce application for computing TF-IDF score of every unique 
* 				 term in a document.
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
import java.util.ArrayList;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
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
* @Class		TFIDF
* @Description	Class representing MapReduce implementation for computing 
* 				TF-IDF score of every unique term in a document.
* 				This class implements static 'TFIDFMap' and 'TFIDFReducer' 
* 				classes. It also holds job configuration for 'TFIDF'.
******************************************************************************/
public class TFIDF extends Configured implements Tool {

	private static final String INTERMEDIATE_OUTPUT_PATH = "intermediate_output";

	/******************************************************************************
	* @StaticClass	TFIDFMap
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<Term>, <Filename>=<TermFrequency>’.
	******************************************************************************/
	public static class TFIDFMap extends Mapper<LongWritable, Text, Text, Text> {

		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
							throws IOException, InterruptedException {
			String term;
			String frequency;

			// Process every input line and find out <Term> and 
			// <Filename>=<TermFrequency>.
			term 	  = lineText.toString().split("&#&#&")[0];
			frequency = lineText.toString().split("&#&#&")[1].replace("\t", "=");
			
			context.write(new Text(term), new Text(frequency));
		}
	}

	/******************************************************************************
	* @StaticClass	TFIDFReducer
	* @Description	Reduce class for computing Inverse Document Frequency and 
	* 				TF-IDF score.
	******************************************************************************/
	public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		/* Overridden reduce method */
		@Override 
		public void reduce(Text term, Iterable<Text> postings, Context context)
								throws IOException, InterruptedException {
			ArrayList<String> doc 			= new ArrayList<String>();
			ArrayList<String> termFrequency = new ArrayList<String>();
			double docFrequency = 0;
			double totalCollection;
			double inverseDocFrequency;
			String currentTerm;
			double score;

			// Iterate through posting entries of particular term and find document 
			// frequency.
			for (Text posting : postings) {
				doc.add(posting.toString().split("=")[0]);
				termFrequency.add(posting.toString().split("=")[1]);
				docFrequency += 1;
			}

			// Get the total number of documents in a collection using 
			// 'TotalCollection' parameter set in the job configuration.
			Configuration conf = context.getConfiguration();
			totalCollection    = conf.getDouble("TotalCollection", 1.0);
			
			// Compute the inverse document frequency.
			inverseDocFrequency = Math.log10(1 + (totalCollection / docFrequency));
			
			// Compute the TF-IDF score.
			for (int i=0; i<docFrequency; i++) {
				currentTerm = term.toString() + "&#&#&" + doc.get(i);
				score 		= Double.parseDouble(termFrequency.get(i)) * inverseDocFrequency;
				context.write(new Text(currentTerm), new DoubleWritable(score));
			}
		}
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 3. It consists 
	* 									input path, intermediate path for term 
	* 									frequencies and output path for TF-IDF
	* 									scores.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		// Find the total number of documents in a collection.
		FileSystem fs 		 = FileSystem.get(getConf());
		ContentSummary cs 	 = fs.getContentSummary(new Path(args[0]));
		long totalCollection = cs.getFileCount();

		// Set 'TotalCollection' parameter with the total number of documents 
		// in a collection.
		Configuration conf = new Configuration();
		conf.setDouble("TotalCollection", (double) totalCollection);

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" TFIDF ");
		job.setMapperClass(TFIDFMap.class);
		job.setReducerClass(TFIDFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	/* Entry point */
	public static void main(String[] args) throws Exception {
		String input;
		String output;
		String TFPath;
		int TFStatus;
		int TFIDFStatus;
		
		// Read input and output paths from user.
		input  = args[0];
		output = args[1];
		TFPath = INTERMEDIATE_OUTPUT_PATH + "/TF";
		
		// First, call driver function of 'TermFrequency' class and 
		// then call driver function of 'TFIDF' class.
		TFStatus = ToolRunner.run(new TermFrequency(), new String[] {input, TFPath});
		if (TFStatus == 0) {
			TFIDFStatus = ToolRunner.run(new TFIDF(), new String[] {input, TFPath, output});
			System.exit(TFIDFStatus);
		} else {
			System.exit(TFStatus);
		}
	}

}

