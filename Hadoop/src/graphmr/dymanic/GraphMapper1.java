package graphmr.dymanic;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


  public  class GraphMapper1 extends Mapper<Object, Text, LongWritable, Text>{
    
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
    	  
    		/*
    		 * If the color is red make it as blue and make it's destination as red.
    		 * This means that the traversing will move ahead one node
    		 */
    	  if(color.equals("R"))
    	  {
    		  context.write(node_key,new Text(arr[0]+","+arr[1]+","+arr[2]+","+arr[3]+"|"+arr[0]+",NB"));
    		  
    		  if(!contains(arr[3],arr[1]))
    			  context.write(new LongWritable(new Long(arr[1])),new Text(arr[1]+","+arr[1]+","+arr[2]+","+arr[3]+"|"+arr[0]+",R"));
    		  
    	  }
    	  //If it is not red forward it as it is.(pass B and G nodes unconditionally)
    	  else         
    	  {
    		   context.write(node_key, new Text(str1[1]));
    	  }
    }

  }
    //This function is used to check if the next node is already processed by the traversal.
    //This helps to avoid the cyclic nature of the graph.
    private boolean contains(String str,String sample)
	 {
		 boolean isContains=false;
		 
		 if(str.contains("|"+sample+"|"))
			 isContains=true;
		 else if(str.endsWith("|"+sample))
			 isContains=true;
		 else if(str.startsWith(sample+"|"))
			 isContains=true;
		 else if(str.equals(sample))
			 isContains=true;
		 
		 return isContains;
	 }
    
  } //GraphMapper1
  
