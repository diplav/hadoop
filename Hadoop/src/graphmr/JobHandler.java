package graphmr;
import graphmr.GraphMapper;
import graphmr.MapReduce2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobHandler {
	  private String DIR_HADOOP=null,PORT_HADOOP=null;
	  private Configuration conf=null;
	  private FileSystem fs=null;
	 
	 public JobHandler(String DIR_HADOOP,String PORT_HADOOP) {
		 this.DIR_HADOOP=DIR_HADOOP;
		 this.PORT_HADOOP=PORT_HADOOP;
		 conf = new Configuration();
		 conf.set("fs.default.name", DIR_HADOOP+":"+PORT_HADOOP);
		
	}

	 public JobHandler() {
		 this.DIR_HADOOP="hdfs://localhost";
		 this.PORT_HADOOP="8080";
		 conf = new Configuration();
		 conf.set("fs.default.name", DIR_HADOOP+":"+PORT_HADOOP);
	}
	 
	 Configuration getConf()
	 {
		 return conf;
	 }
	 
	 
	 FileSystem getFileSystem()
	 {
		if(fs==null)
		{
			 try {
				 		fs=FileSystem.get(conf);
			 	 } 
			 catch (IOException e) {
			 		 e.printStackTrace();
			}
		}
		return fs;
	 }
	 
	 public static class TGraphReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

		    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		    {
		    	for(Text t:values)
		    		context.write(key, new Text(t.toString()+"??"));
		    	
		    }
	  }
	 
	 public Job getFirstJob(String job_name,String input_path,String output_path)
	 {
		 Job job = null;
		  try {
			  job = new Job(conf, job_name);
		    job.setJarByClass(MapReduce2.class);
		    job.setMapperClass(GraphMapper.class);
		    //job.setCombinerClass(GraphReducer.class);
		    job.setReducerClass(GraphReducer.class);
		    
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setNumReduceTasks(10);
		    
				FileInputFormat.addInputPath(job, new Path(input_path));
			    FileOutputFormat.setOutputPath(job,new Path(output_path));
		    } catch (IllegalArgumentException | IOException e) {
				// 
				e.printStackTrace();
			}
		
		   return job;
	 }
	 
	
	 
	 public Job getJob(String job_name,String input_path,String output_path)
	 {
		 Job job1 = null;
		 try{
		 job1=new Job(conf, job_name);
		 job1.setJarByClass(MapReduce2.class);
		 job1.setMapperClass(GraphMapper1.class);
		 job1.setReducerClass(GraphReducer.class);
		 job1.setOutputKeyClass(LongWritable.class);
		 job1.setOutputValueClass(Text.class);
		 job1.setMapOutputKeyClass(LongWritable.class);
		 job1.setMapOutputValueClass(Text.class);
		 job1.setNumReduceTasks(10);
		 FileInputFormat.addInputPath(job1, new Path(input_path));
		 FileOutputFormat.setOutputPath(job1,new Path(output_path));
		 }catch(IOException e)
		 {
			 e.printStackTrace();
		 }
		 return job1;
	 }
	 
}
