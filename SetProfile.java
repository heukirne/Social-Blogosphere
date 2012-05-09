import com.mongodb.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import java.util.*;
import java.net.*;
import java.io.*;
 
public class SetProfile {
 
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=&password=";
	public static Mongo mongoConn;
	public static DB mongoDb;
	public static DBCollection collPosts;
	public static Connection mysqlConn;
	public static Statement myStm;
	
    public static void main(String[] args) throws Exception {		
	
Properties configFile = new Properties();
configFile.load( new FileInputStream("my_config.properties"));
myConnString = configFile.getProperty("MYCONN");

		mongoConn = new Mongo( configFile.getProperty("MYHOST") , 27017 );
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
DBCollection collList = mongoDb.getCollection("pageRank_"+args[0]);

/* Populate Importante Authors  /	
BasicDBObject sortDocP = new BasicDBObject();
sortDocP.put("value.pr", -1);
        
DBCursor cur = collList.find().sort(sortDocP).limit(10000);
while(cur.hasNext()) {
  DBObject obj = cur.next();
  myStm.executeUpdate("UPDATE author SET views = -1 where views is null and profileID = '" + obj.get("_id").toString() + "' LIMIT 1");
}	
/**/

/* Populate Mongo Authors */
BasicDBObject doc = new BasicDBObject();
myStm.executeQuery("SELECT profileID, views FROM author WHERE views > 0;");
ResultSet rs = myStm.getResultSet();
while (rs.next()) {
	doc = new BasicDBObject();
	doc.put("_id",rs.getString("profileID"));
	
	BasicDBObject author = (BasicDBObject)collList.findOne(doc);
	if (author!=null) {
		author.put("views",rs.getInt("views"));
		collList.save(author);
	}
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
