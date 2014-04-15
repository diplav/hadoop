package graphmr.dymanic;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	  
	  public  class DataCombinerMapper extends Mapper<Object, Text, LongWritable, Text>{
	    
	    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    
	    	LongWritable node_key = new LongWritable();
	    	
	    	  //tokenize the input file line by line
	    	StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
	      
	    	//for each line/record
	    	while (itr.hasMoreTokens()) {  
	    		//get the next record which in the format "key \t source,destination,cost,path,color" ..(\t is delimiter between key and record)
	    		String str=itr.nextToken();
	    		//split key and record
	    		String str1[]=str.split("\t");
	    	    //get all the fields from record
	    	    String arr[] = str1[1].split(",");
	    		node_key.set(new Long(arr[0]));   //set key
	    	    //color is the last field in the record
	    		String color=arr[arr.length-1];   //get color
	    	  if(!color.equals("RR")) 
	    		  context.write(node_key,new Text(str1[1]));
	    	
	    }

	  }
} //DataMapper
	 
	