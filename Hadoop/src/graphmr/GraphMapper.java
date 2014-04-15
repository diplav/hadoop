package graphmr;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

  public  class GraphMapper extends Mapper<Object, Text, LongWritable, Text>{

	  //this will be the starting node
	  String start_node="8";
     
      public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
    	  
    	  LongWritable node_key = new LongWritable();
    	  
    	  //tokenize the input file line by line
    	  StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
    	  
    	  //iterate for each record present in the input file
    	  while (itr.hasMoreTokens()) {
    	   
    		  //get the next record and fetch all the  fields delimited by ','
    		  String str=itr.nextToken();
    		  StringTokenizer itr1 = new StringTokenizer(str,",");
    		  //get the first field of the record and use it as key
    		  String temp_key=itr1.nextToken();
    		  node_key.set(new Long(temp_key)); //this is key
    	  
    	  //if the given record(edge) is from the starting node 	  
    	  if(temp_key.equals(start_node))
    	  {
    		 String arr[] = str.split(",");
    		 context.write(new LongWritable(new Long(arr[1])),new Text(arr[1]+",,"+arr[2]+","+arr[0]+"|"+arr[1]+",R"));
    	  }
    	  else
    	  {
    		 context.write(node_key,new Text(str+",,G"));
    		  
    	  }//else       
      } //while
     
    }//map function

  }//map class
