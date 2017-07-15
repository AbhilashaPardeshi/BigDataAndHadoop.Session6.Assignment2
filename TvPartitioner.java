package Assignment2;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author abhilasha
 *
 */
public class TvPartitioner extends Partitioner<LongWritable,Text> implements Configurable
{
	int iAvg = 0;
	
	@Override
	public int getPartition(LongWritable key, Text value, int iNumOfPartitions) 
	{
		int iPartition = 0;
		if (key.get()>=iAvg) 
		{
			iPartition = 1;
		}
		System.out.println("Partition ["+key.get()+" , "+iPartition+"]");
		
		return iPartition;
	}

	@Override
	public Configuration getConf() 
	{
		return null;
	}

	@Override
	public void setConf(Configuration conf) 
	{
		System.out.println("Partition : COnfiguration object is "+conf.get("AvgUnits"));
		
		//Extracting avg units from congiguration object
		iAvg = Integer.parseInt(conf.get("AvgUnits"));
	}

}
