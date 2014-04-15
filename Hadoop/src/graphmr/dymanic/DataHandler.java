package graphmr.dymanic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataHandler {
	static String fetch_score="select d.score from future_data d,mapping m where m.src=? and m.dst=? and d.id=? and m.src_edge=edge_id";
	static double default_score=50;
	private Connection conn=null;
	private long start_id=1;
	
	public DataHandler(long start_id)
	{
		conn=Database.connect();
		this.start_id=start_id;
	}
	
	public long computeId(int score)
	{
		double d=score;
		if(score==0)
			return start_id;
		return (long)(Math.ceil(d/default_score));
		
	}
	public int getValue(int score,long source,long destination)
	{
		int scr=0;

		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement(fetch_score);
			pstmt.setLong(1, source);
			pstmt.setLong(2, destination);
			pstmt.setLong(3, computeId(score));
			ResultSet rs=pstmt.executeQuery();	

			if(rs.next())
				scr=rs.getInt(1);


		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return scr;
	}
	
	public String correctData(FileSystem fs,String input_file)
	{
		if(fs==null)
			System.out.println("fs is null in function correctData");
		String base_path=fs.getWorkingDirectory()+"/temp/";
		
		FSDataOutputStream out=null;
		FSDataInputStream in=null;
		
		try {
			in= fs.open(new Path(input_file));
			out = fs.create(new Path(base_path+"temp_data.txt"));
			String line=in.readLine();
			while(line!=null)
			{
				String arr[]=line.split("\t");
				String tarr[]=arr[1].split(",");
				
				String scores[]=tarr[2].split("\\+");
				//tarr[0]: dst,tarr[1]:src,scores[0]:score
				int score_val=new Integer(scores[0]);
				int val=getValue(score_val,new Long(tarr[1]),new Long(tarr[0]));
				//System.out.println("src:"+tarr[1]+" Destination: "+tarr[0]+" score: "+scores[0]+" val:"+val+" id:"+computeId(new Integer(scores[0])));
				score_val=score_val+val;
				
				String ll=arr[0]+"\t"+tarr[0]+","+tarr[0]+","+score_val+","+tarr[3]+",R\n";
				
				out.write(ll.getBytes());
				line=in.readLine();
			}
			
			in.close();
			out.close();
			
		} catch (IllegalArgumentException | IOException e) {
			
			e.printStackTrace();
		}

	 return base_path+"temp_data.txt";	
	}

}
