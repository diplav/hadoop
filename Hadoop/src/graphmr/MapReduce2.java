package graphmr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;



public class MapReduce2 {
 
	JobHandler handler=null;
	FileSystem fs=null;
	int status=0;
/*
 public static class GraphReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

	    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
	    	for(Text t:values)
	    		context.write(key, t);
	    	
	    }
  }*/
  
	
  public static void main(String[] args) throws Exception {
	  MapReduce2 mp=new MapReduce2();
	  try{
		  mp.runMapReduce();
	  		}
	  		catch(Exception e){
	  			e.printStackTrace();
	  		}
  }
  
  
  public MapReduce2()
  {
	   handler=new JobHandler("hdfs://localhost","9000");
	   fs = handler.getFileSystem();
  }
  
  
  public Counter runFirstJob() throws ClassNotFoundException, IOException, InterruptedException
  {
	  Counter c1=null;
	  String basic_input_path="hdfs://localhost:9000/user/diplav/graph/";
	  String basic_output_path="hdfs://localhost:9000/user/diplav/output0";
	  
	  Job job =handler.getFirstJob("Graph_First", basic_input_path, basic_output_path);  
	  status=job.waitForCompletion(true)?0:1;  //run first job
	  c1 =  job.getCounters().findCounter(GRAPH_COUNTER.DONE);
	  System.out.println("First Job Completed: status= "+status); 
	  return c1;
  }
  
  public Counter runJob(String job_name,String input_path,String output_path) throws ClassNotFoundException, IOException, InterruptedException
  {
	  Counter c1;
	  Job job1 = handler.getJob(job_name, input_path, output_path);  
	  System.out.println("Job Successfully Completed...? "+job1.waitForCompletion(true));
	   c1 =  job1.getCounters().findCounter(GRAPH_COUNTER.DONE);
	  return c1;
  
  }
  public void runMapReduce() throws ClassNotFoundException, IOException, InterruptedException
  {
	  Counter c1;
	  c1=runFirstJob();
	  
    String base_path="hdfs://localhost:9000/user/diplav";
    
    if(status==0)
    {
    	int count=0;
    
    	String last_dir=null,output_path=null,input_path=null;
    	/*
    	 * This while loop creates iterative mapreduce job based on the feedback of the previous 
    	 * job. First job is compulsory and executed outside this loop.
    	 */
    	while(c1!=null && c1.getValue() > 0)
    	{
    		if(last_dir!=null) //delete the previously operated directory
    			fs.delete(new Path(last_dir), true); // delete file, true for recursive 
	    	   
    		 last_dir=input_path=base_path+"/output"+count+"/";
    	     count++;
    	     output_path=base_path+"/output"+count+"/";
    	
    	     c1=runJob("graph"+count,input_path, output_path);
    	}
		if(last_dir!=null) //delete the previously operated directory
			fs.delete(new Path(last_dir), true); // delete file, true for recursive 
    	
    	if(output_path!=null)
    	{
    	     SimpleDateFormat ft = new SimpleDateFormat ("yyyy_MM_dd_hh_mm_ss");
    	     fs.rename(new Path(output_path), new Path("/user/diplav/result/output_"+ft.format(new Date())));
    		
    	}
    	
    }
    
  }
}