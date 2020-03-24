

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.net.URI;




public class MyIndexer extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration myconf = getConf();


		//myconf.set("mapreduce.framework.name", "local");//local
		//myconf.set("fs.defaultFS", "file:///");//local
		myconf.set("textinputformat.record.delimiter", "\n[[");//special delimiter to break the text into documents

		//in order to put stopwords file from local to hdfs
		FileSystem fs = FileSystem.get(myconf);//hdfs
		fs.copyFromLocalFile(new Path("file:///users/pgt/2486083h/big_data_uog/src/main/resources/stopword-list.txt"),
		new Path("hdfs://bigdata-10.dcs.gla.ac.uk:8020/user/2486083h/stopword-list.txt"));//hdfs

		Job job = Job.getInstance(myconf);
		try { 
			//add stop words file to cache for all mappers to access
			//job.addCacheFile(new URI("./src/main/resources/stopword-list.txt"));//local
			job.addCacheFile(new URI("./stopword-list.txt"));//hdfs
		}
		catch (Exception e) {
			System.out.println("File Not Added");
			System.exit(1);
		}
		// Partitioning and sorting configuration
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(Comparator.class);
		job.setGroupingComparatorClass(CustomGroupComparator.class);


        //General configuration
		job.setJobName("Indexing wikipedia");
		job.setJarByClass(MyIndexer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper configuration
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(FreqDocPair.class);

		// Reducer configuration
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(60);
		//We increase the reducer memory as it is not enough
		myconf.setInt("mapreduce.reduce.memory.mb",1536);
		myconf.setInt("mapreduce.reduce.java.opts",1230);//80%of reducers memory
        //input and output files configuration
		MultipleOutputs.addNamedOutput(job, "postingList", TextOutputFormat.class, Text.class, Text.class);//output foler for posting lists
		MultipleOutputs.addNamedOutput(job, "docToLen", TextOutputFormat.class, Text.class, Text.class);//output folder for document length file
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int res= job.waitForCompletion(true) ? 0 : 1;
		
		//counters
		Counters cn=job.getCounters();
		Long num_of_tokens=cn.findCounter(MyMapper.Counters_index.NUM_TOKENS).getValue();//sum of tokens of whole collection
		Long num_of_docs=cn.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();//mapper input records
		Double avg_doc_length=(num_of_tokens*1.0)/num_of_docs;
		System.out.println("Average document length = "+avg_doc_length);
		System.out.println("Total number of documents = "+num_of_docs);		
		return res;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyIndexer(), args));
	}
}
