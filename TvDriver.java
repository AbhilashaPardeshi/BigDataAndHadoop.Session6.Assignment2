package Assignment2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 
 * @author abhilasha
 *
 */
public class TvDriver
{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		//Check if input parameters provided appropriately
		if(args==null || args.length!=5)
		{
			System.err.println("Incorrect input parameters provided");
			System.exit(-1);
		}
	
		
		//Instantiate configuration object
		Configuration conf=new Configuration();

		int iMaxUnits = Integer.parseInt(args[2]);
		int iMinUnits = Integer.parseInt(args[3]);
		int iNumberOfReducers = Integer.parseInt(args[4]);

		int iAvgUnits = (iMaxUnits+iMinUnits)/iNumberOfReducers;
		conf.set("AvgUnits", String.valueOf(iAvgUnits));
		conf.set("NumOfReducers", String.valueOf(iNumberOfReducers));

		
		//Instantiate job object
		Job job=new Job(conf, "Tv");
		
		/*
		 * Set input pathput
		 *eg : /abhilasha/TVSalesOutput/part-r-00000
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		/*
		 * Set output path
		 * eg :/abhilasha/SortedOutput
		 */
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		//Delete output directory if already existing
		outputPath.getFileSystem(conf).delete(outputPath, true);

		//Set main class to start execution
		job.setJarByClass(TvDriver.class);
	
		//Set mapper class
		job.setMapperClass(TvMapper.class);
		
		//Set default reducer as we need it for sorting only
		job.setReducerClass(Reducer.class);
		
		//Set number of reducers
		job.setNumReduceTasks(iNumberOfReducers);
		
		//Set Partitioner class
		job.setPartitionerClass(TvPartitioner.class);
		
		//Set input and output formats
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set output key value classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		//Wait for job completion
		job.waitForCompletion(true);
	}

}
