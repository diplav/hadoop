package graphmr.dymanic;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	  public  class DataReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
	  
	     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	     {
	     	int min_val=Integer.MAX_VALUE;
	     	String min_str=null;
	     	
	     	//iterate all the values having same key
	     	for (Text val1 : values){
	     		 String val=val1.toString();
	     		 String arr[] = val.split(",");
	     	     String color=arr[arr.length-1];  
	     	     if(color.equals("RR")){
	     	    	String tarr[]=arr[2].split("\\+");
	     	    	 
	     	    	 Integer temp=new Integer(tarr[0]);
	     	    	 if(temp < min_val)
	     	    	 {
	     	    		 min_val=temp;
	     	    		 min_str=val;
	     			
	     		    }    
	     	  }
	     	  	    	  
	       }
	       if(min_str!=null)  //we have found the minimal path for update 
	 		  context.write(key, new Text(min_str));
	 	  
	   }
	     
}