package graphmr.dymanic;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;

public class MapReduce3 {
 
	JobHandler handler=null;
	DataHandler dhandler=null;
	FileSystem fs=null;
	int status=0;
	
  public static void main(String[] args) throws Exception {
	  //MapReduce3 mp=new MapReduce3("hdfs://localhost","9000");
	  MapReduce3 mp=new MapReduce3();
	  try{
		  mp.runMapReduce();
	  		}
	  		catch(Exception e){
	  			e.printStackTrace();
	  		}
  }
  
  public MapReduce3(String HDFS_PATH,String HDFS_PORT)
  {
	   handler=new JobHandler(HDFS_PATH,HDFS_PORT);
	   fs = handler.getFileSystem();
	   dhandler=new DataHandler(0); //start_id is '0'
  }
   
  public MapReduce3()
  {
	   handler=new JobHandler("hdfs://localhost","9000");
	   fs = handler.getFileSystem();
	   dhandler=new DataHandler(0); //start_id is '0'
  }
  
  public Counter runFirstJob() throws ClassNotFoundException, IOException, InterruptedException
  {
	  Counter c1=null;
	  String basic_input_path="hdfs://localhost:9000/user/diplav/graph/";
	  String basic_output_path="hdfs://localhost:9000/user/diplav/output0";
	  
	  Job job =handler.getFirstJob("Graph_First", basic_input_path, basic_output_path);  
	  status=job.waitForCompletion(true)?0:1;  //run first job
	  c1 =  job.getCounters().findCounter(GRAPH_COUNTER.DONE);
	  System.out.println("First Job Completed: status= "+status+"  Counter Value:"+c1.getValue()); 
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
    
    	String last_dir=null,output_path=null,input_path=null,temp_path=null;
    	/*
    	 * This while loop creates iterative mapreduce job based on the feedback of the previous 
    	 * job. First job is compulsory and executed outside this loop.
    	 */
    	while(c1!=null && c1.getValue() > 0)
    	{
    		if(last_dir!=null) //delete the previously operated directory
    			fs.delete(new Path(last_dir), true); // delete file, true for recursive 
	    	   
    		 last_dir=input_path=base_path+"/output"+count+"/";
    		 temp_path=base_path+"/temp/"+"temp_"+count+"/";
    	     count++;
    	     output_path=base_path+"/output"+count+"/";	      
    	     
    	     if(c1!=null && c1.getValue() > 0)
    	     {
    	    	
    	    	 ArrayList<String> paths= new ArrayList<String>();
    	    	//this data job
        	     handler.getDataJob("data_job", input_path, temp_path).waitForCompletion(true);
        	     //this will load data from mysql server
    	    	paths.add( dhandler.correctData(fs, temp_path+"part-r-00000"));
    	    	//fs.rename(new Path(temp_path+"part-r-00000"), new Path(base_path+"/lonely/part"+count));
    	    	
    	    	fs.delete(new Path(temp_path));
    	    	
    	    	paths.add(input_path);
    	    	String output=base_path+"/datatemp/output_temp";
    	    	handler.getDataCombinerJob("data combiner",paths,output).waitForCompletion(true);
    	    	fs.delete(new Path(input_path), true); // delete file, true for recursive 
    	    	//fs.rename(new Path(input_path), new Path(base_path+"/lonely/output"+count+"/"));
    	    	fs.rename(new Path(output), new Path(input_path));
    	    	 
    	     }
    	     
    	     c1=runJob("graph"+count,input_path, output_path);
    	     //fs.rename(new Path(input_path), new Path(base_path+"/lonely/output"+count+"/"));
    	}
		if(last_dir!=null) //delete the previously operated directory
			fs.delete(new Path(last_dir), true); // delete file, true for recursive 
    	
    	if(output_path!=null)
    	{
    	     SimpleDateFormat ft = new SimpleDateFormat ("yyyy_MM_dd_hh_mm_ss");
    	     String path_final="/user/diplav/result/output_"+ft.format(new Date()); 
    	     handler.getResultFilterJob("result_filter",output_path,path_final+"/").waitForCompletion(true);
    	     
    	     fs.delete(new Path(output_path),true);
    		
    	}
    	
    }
    
  }
}