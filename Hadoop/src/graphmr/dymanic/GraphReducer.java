package graphmr.dymanic;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 public  class GraphReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
 
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
    	/*
    	 * found: used to know that the minimum path is found or not.
    	 * next: used to decide whether we should do the next map reduce phase.
    	 * move_ahead: whether we should move the current traversal to the next edge.
    	 *             This could be happens only if at the first traversal or if new 
    	 *             minimal/shortest path found in the subsequent phases. 
    	 */
    	boolean found=false,next=false,move_ahead=false;
    	
    	String path=null;
    	int min_val=Integer.MAX_VALUE;
    	/*
    	 * "values" is the enumeration and we can traverse it only once. 
    	 * first traverse all values to find the current minimal path and also store
    	 * the records that will be emitted as output in "array".
    	 */
    	
    	ArrayList<String> array=new ArrayList<String>();
    	
    	String min_str=null;
    	
    	//iterate all the values having same key
    	for (Text val1 : values){
    		
    		 String val=val1.toString();
    		 String arr[] = val.split(",");
    		 //get the color of node, it is last attribute
    	     String color=arr[arr.length-1];
    	     
    	   //if the color is not green  
    	     if(color.equals("G")||color.equals("G\n")){
    	    	 array.add(val1.toString()); //unconditionally add green nodes 
    	     }
    	     else{
    	    	 Integer temp=new Integer(arr[2]);
    	    	 if(temp < min_val)
    	    	 {
    	    		 path=arr[3];
    	    		 min_val=temp;
    	    		 min_str=val;
    			
    			if(color.equals("R")||color.equals("NB"))   //if newly identified path is optimal move ahead
    				move_ahead=true;
    			else 
    				move_ahead=false;
    			
    		  }
    	      found=true;     //we have found some minimal path 
    	  }
    	  
   	  
      }
      if(min_str!=null)  //we have found the minimal path
	  {
		  String arr[]=min_str.split(",");
		  context.write(key, new Text(arr[0]+","+arr[0]+","+arr[2]+","+arr[3]+","+"B"));
	  }
      
      //if minimal path is not found then insert  only green nodes
      if(!found || !move_ahead){ //green record will always be there
    	  	  for (String str : array)   //add all the green nodes in the output 		 
        		  context.write(key,new Text(str));  
      }
      //if newly minimal path is found
      else if(move_ahead){
    	   	  
    		  for (String str1 : array){
    			  String str=new String(str1);
    			  String tarr[] = str.toString().split(",");
    			  //get color
    			  String color=tarr[tarr.length-1]; 
    			  //we will not touch Blue nodes	
    			  if(color.equals("B")) {
    				   context.write(key,new Text(str1));
    			  }
    			  else { //process for all green records
        			  int final_score = new Integer(tarr[2]);
        			  
        			  if(!contains(path,tarr[1])){
        				  context.write(new LongWritable(new Long(tarr[1])),new Text(tarr[1]+","+tarr[0]+","+min_val+"+"+final_score+","+path+",RR"));
        				  next=true;
        			  }
        			  //this will write green records
        			  context.write(key, new Text(str1));
        	      }//else
    	      } //for
       }//else if
      
      if(next) //this decides..whether to run the next mapreduce stage
    	  context.getCounter(GRAPH_COUNTER.DONE).increment(1L);
    	  
      
  }
    
    private boolean contains(String str,String sample)
	 {
		 boolean isContains=false;	 
		 if(str.contains("|"+sample+"|"))  //check if it is middle
			 isContains=true;
		 else if(str.endsWith("|"+sample))  //check if it is at the end
			 isContains=true;
		 else if(str.startsWith(sample+"|")) //check if it is at the start
			 isContains=true;
		 else if(str.equals(sample))         //currently this condition is not possible i.e. path and node are equal
			 isContains=true;
		 
		 return isContains;
	 }  
  }
 
