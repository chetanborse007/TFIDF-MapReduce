/******************************************************************************//*!
* @File          Search.java
* 
* @Title         MapReduce application for a simple batch mode search engine that
* 				 accepts as input a user query and outputs a list of documents with
* 				 scores that best matches the query (a.k.a  search hits ).
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
* @Class		Search
* @Description	Class representing MapReduce implementation for a simple batch
* 				mode search engine that accepts as input a user query and 
* 				outputs search hits.
* 				This class implements static 'SearchMap' and 'SearchReducer' 
* 				classes. It also holds job configuration for 'SearchEngine'.
******************************************************************************/
public class Search extends Configured implements Tool {

	/******************************************************************************
	* @StaticClass	SearchMap
	* @Description	This map class first checks if one of the user query terms 
	* 				matches the term in vocabulary extracted through TFIDF program
	* 				and then accordingly outputs a key-­value pair such as
	* 				‘<Filename>, <TFIDF Score>’.
	******************************************************************************/
	public static class SearchMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
							throws IOException, InterruptedException {
			String key;
			double score;
			String term;
			String doc;
			String query;

			// Parse each line and get information such as term, document and 
			// TFIDF score.
			key   = lineText.toString().split("\t")[0];
			score = Double.parseDouble(lineText.toString().split("\t")[1]);
			term  = key.split("&#&#&")[0];
			doc   = key.split("&#&#&")[1];
			
			// Get the user query using 'Query' parameter set in the job 
			// configuration.
			Configuration conf = context.getConfiguration();
			query    		   = conf.get("Query").toLowerCase();
			query 			   = query.replaceAll("\\t", " ");

			// Check if one of the user query terms matches the term in vocabulary
			// extracted through TFIDF program and then accordingly output a 
			// key-­value pair such as ‘<Filename>, <TFIDF Score>’.
			for (String queryTerm : WORD_BOUNDARY.split(query)) {
				queryTerm = queryTerm.trim();
				if (!queryTerm.isEmpty() && term.equals(queryTerm)) {
					context.write(new Text(doc), new DoubleWritable(score));
				}
			}
		}
	}

	/******************************************************************************
	* @StaticClass	SearchReducer
	* @Description	Reduce class that accumulates the scores of each document and
	* 				outputs the document name along with its accumulated score.
	******************************************************************************/
	public static class SearchReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		/* Overridden reduce method */
		@Override 
		public void reduce(Text doc, Iterable<DoubleWritable> scores, Context context)
								throws IOException, InterruptedException {
			double docScore = 0.0;

			// Accumulate the scores of each document.
			for (DoubleWritable score : scores) {
				docScore += score.get();
			}

			context.write(new Text(doc), new DoubleWritable(docScore));
		}
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 3. It consists 
	* 									a user query, input path for TF-IDF 
	* 									scores and output path for search 
	* 									results.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		// Set 'Query' parameter with the user query.
		Configuration conf = new Configuration();
		conf.set("Query", args[0]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" SearchEngine ");
		job.setMapperClass(SearchMap.class);
		job.setReducerClass(SearchReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	/* Entry point */
	public static void main(String[] args) throws Exception {
		String query;
		String input;
		String output;
		int SearchStatus;
		
		// Read from user the user query, input path for TF-IDF scores 
		// and output path for search results.
		query  = args[0];
		input  = args[1];
		output = args[2];
		
		// Call driver function.
		SearchStatus = ToolRunner.run(new Search(), new String[] {query, input, output});

		System.exit(SearchStatus);
	}

}

