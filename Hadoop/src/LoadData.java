import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class LoadData {
	  public static final String DIR_HADOOP = "hdfs://192.168.18.23";

	    public static final String PORT_HADOOP = "9000";
	  
	  static void usage () {
	    System.out.println("Usage : HadoopDFSFileReadWrite <inputfile> <output file>");
	    System.exit(1);
	  }
	  
	  static void printAndExit(String str) {
	    System.err.println(str);
	    System.exit(1);
	  }

	  public static void main (String[] argv) throws IOException {
	    Configuration conf = new Configuration();
	    conf.set("fs.default.name", DIR_HADOOP + ":" + PORT_HADOOP);
	    FileSystem fs = FileSystem.get(conf);
	    String base_path=fs.getWorkingDirectory() + "/graph/";

	    
	    for(int i=0;i<1;i++)
	    {
	    
	    // Hadoop DFS deals with Path
	  
	    	boolean not_done=true;
		    int count=0;
	    	while(not_done)
		    {	
	    Path outFile = new Path(base_path+"mat_"+i+"_"+count+".txt");
	    File file = new File("/usr/local/backup/mat_"+i+"_"+count+".txt");
	    
	    if(!file.exists())
	    	break;
	    count++;
	  
	    DataInputStream dis = new DataInputStream(new FileInputStream(file));
	    
	    if (fs.exists(outFile))
	      printAndExit("Output already exists");

	    FSDataOutputStream out = fs.create(outFile);
	    byte buffer[] = new byte[1024];
	    try {
	      int bytesRead = 0;
	      while ((bytesRead = dis.read(buffer)) > 0) {
	        out.write(buffer, 0, bytesRead);
	      }
	    } catch (IOException e) {
	      System.out.println("Error while copying file");
	    } finally {
	      dis.close();
	      out.close();
	    }//finally
	    
		    }//while
	    
	    }//for loop
	  }
	  
}
