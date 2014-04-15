package graphmr.dymanic;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	  public  class DataCombinerReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
	  
		  public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		    {
		    	for(Text t:values)
		    		context.write(key, t);
		    	
		    }
	     
}