import com.mongodb.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import java.util.*;
import java.net.*;
import java.io.*;
 
public class SetStats {
 
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=root&password=";
	public static Mongo mongoConn;
	public static DB mongoDb;
	public static DBCollection collPosts;
	public static Connection mysqlConn;
	public static Statement myStm;
	
    public static void main(String[] args) throws Exception {		
	
		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}

		collPosts = mongoDb.getCollection("posts");
		collPosts.ensureIndex("postID");
		collPosts.ensureIndex("blogID");
		collPosts.ensureIndex("authorID");		
		
		try {
		mysqlConn = DriverManager.getConnection(myConnString);
		myStm = mysqlConn.createStatement();
		myStm.executeQuery("set wait_timeout = 7200");
		} catch (Exception e) {
			System.out.println("MySQL Offline.");
			System.exit(1);
		}
		
        int blogsTotal = 0;

myStm.executeQuery("SELECT sum(1+length(blogs)-length(replace(blogs,',',''))) as cont FROM author WHERE length(blogs) > 5 and Local = 'BR' AND retrieve=1");
ResultSet rs = myStm.getResultSet();
rs.next();
blogsTotal = rs.getInt("cont");

String mapBlogs =	"function(){ " +
"	emit( this.blogID , this.comments.length ); "+
"	};";							

String mapCountBlogs = "function () { emit ( this.blogID, 1 ); };";
	
String reduceAvg = "function( key , values ){ "+
" var totCom = 0; " +
" for ( var i=0; i<values.length; i++ ) {"+
"	totCom += parseInt(values[i]); "+
" } " +
" return totCom; };";

String reduceCountBlogs = "function( key, values ) { var tot=0; values.forEach(function(value) {tot+=value;}); return tot; };";

QueryBuilder query = new QueryBuilder();
DBObject docMin = query.start("comments").notEquals(new BasicDBList()).get();
DBObject docPop = query.start("value.avg").greaterThanEquals(1).get();

//MapReduceOutput output = collPosts.mapReduce(mapBlogs, reduceAvg, "blogStats", docMin);
//output.getOutputCollection().rename("blogStats",true); //Workaround for Mongo 1.6.3
//MapReduceOutput output2 = collPosts.mapReduce(mapCountBlogs, reduceCountBlogs, "blogCount", null);
//output2.getOutputCollection().rename("blogCount",true);

	DBCollection collBlogCount = mongoDb.getCollection("blogCount");
	DBCollection collResult = mongoDb.getCollection("blogStats");

        long blogsLive = collResult.getCount();
	long blogsActive = collBlogCount.getCount();
	long blogsLonely = blogsActive - blogsLive;

		long blogsPop = collResult.getCount(docPop);
		int blogsInactive = blogsTotal - (int)blogsActive;
		
		String sql = "UPDATE blogStats SET " +
					"total = " + blogsTotal + "," +
					"active = " + blogsActive + "," +
					"inactive = " + blogsInactive + "," +
					"live = " + blogsLive + ", " +
					"popular = " + blogsPop + ", " +
					"lonely = " + blogsLonely + " LIMIT 1";	
		if (blogsActive!=0) myStm.executeUpdate(sql);
		
		System.out.println(">>> Blogs Inactive: " + blogsInactive );
		
		//System.out.println("Mongo Users Posted: " + collPosts.distinct("authorID").size() );
		
		System.out.println("Mongo Blogs Filled: " + blogsActive );
		System.out.println("Mongo Blogs With Comments: " + blogsLive );
		System.out.println("Mongo Blogs Empty Comments: " + blogsLonely );
		System.out.println("Mongo Blogs Popular: " + blogsPop );
		
		System.out.println("Mongo Posts: " + collPosts.getCount() );	
		//System.out.println("Mongo Tags: " + collPosts.distinct("tags").size() );
		
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
