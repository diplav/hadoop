package graphmr.dymanic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class Database {
	
	
	public static Connection connect(){
		 Connection conn=null;
	    try {
	      Class.forName("com.mysql.jdbc.Driver").newInstance();
	      conn = DriverManager.getConnection("jdbc:mysql://LOCALHOST:3306/graph","root","root");
	      System.out.println("Connected to mysql database");


	    } catch (SQLException ex) {
	      // handle any errors
	      System.out.println("SQLException: " + ex.getMessage());
	      System.out.println("SQLState: "  + ex.getSQLState());
	      System.out.println("VendorError: " +  ex.getErrorCode());
	    }catch(Exception e){e.printStackTrace();}   
	  
	return conn;
	}
	
}
