import com.mongodb.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import java.util.*;
import java.net.*;
import java.io.*;
 
public class SetBlogs {
 
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=&password=";
	public static Mongo mongoConn;
	public static DB mongoDb;
	public static Connection mysqlConn;
	public static Statement myStm;
	
    public static void main(String[] args) throws Exception {		
	
		mongoConn = new Mongo( "143.54.12.117" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}

		try {
		mysqlConn = DriverManager.getConnection(myConnString);
		myStm = mysqlConn.createStatement();
		myStm.executeQuery("set wait_timeout = 7200");
		} catch (Exception e) {
			System.out.println("MySQL Offline.");
			System.exit(1);
		}
	
/* Populate Blogs  */

DBCollection collBlogs = mongoDb.getCollection("blogStats");

DBCursor cur = collBlogs.find().limit(1);
while(cur.hasNext()) {
  DBObject obj = cur.next();
  //myStm.executeUpdate("INSERT INTO blogs SET blogID = '" + obj.get("_id").toString() + "';");
}
/**/


	
        shutdown();
    }
	
    private static void shutdown()
    {
		System.out.println( "Shutting down database ..." );
		mongoConn.close();
		try {
			myStm.close();
		} catch (Exception ex) {}
    }
	
}
