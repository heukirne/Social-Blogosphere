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
 
	public static final String myConnString = "jdbc:mysql://143.54.12.###/bloganalysis?user=profile&password=profile";
	public static Mongo mongoConn;
	public static DB mongoDb;
	public static DBCollection collPosts;
	public static Connection mysqlConn;
	public static Statement myStm;
	
    public static void main(String[] args) throws Exception {		
	
		mongoConn = new Mongo( "143.54.12.###" , 27017 );
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

myStm.executeQuery("SELECT sum(1+length(blogs)-length(replace(blogs,',',''))) as cont FROM author WHERE length(blogs) > 5 and Local = 'BR' AND retrieve>0");
ResultSet rs = myStm.getResultSet();
rs.next();
blogsTotal = rs.getInt("cont");

String mapBlogs =	"function(){ " +
"	emit( this.blogID , { posts: 1 , comments: this.comments.length } ); "+
"	};";							

String mapHistory =       "function(){ " +
" 	var day = this._id.getTimestamp().getFullYear() + '.';"+
"	day += this._id.getTimestamp().getMonth() + '.';"+
"	day += this._id.getTimestamp().getDate();"+
"       emit( day , 1 ); "+
"  };";

String reduceHistory = "function( key , values ){ "+
" var totCom = 0; " +
" values.forEach(function(value) {"+
" 	totCom += value;"+
" });"+
" return totCom; };";


String reduceBlogs = "function( key, values ) { "+
"var result = { posts:0, comments:0 };"+
"values.forEach(function(value) {"+
	"result.posts += value.posts;"+
	"result.comments += parseInt(value.comments);"+
"}); return result; };";

QueryBuilder query = new QueryBuilder();
DBObject docMin = query.start("numComments").greaterThanEquals(1).get();
DBObject docPop = query.start("value").greaterThanEquals(25).get();

DBObject docQuery = query.start("numComments").greaterThanEquals(20).get();
MapReduceOutput output = collPosts.mapReduce(mapBlogs, reduceBlogs, "blogStats", MapReduceCommand.OutputType.REPLACE, null);
//MapReduceOutput output = collPosts.mapReduce(mapHistory, reduceHistory, "history", MapReduceCommand.OutputType.REPLACE ,null);

/*
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
		
		System.out.println("Mongo Blogs Filled: " + blogsActive );
		System.out.println("Mongo Blogs With Comments: " + blogsLive );
		System.out.println("Mongo Blogs Empty Comments: " + blogsLonely );
		System.out.println("Mongo Blogs Popular: " + blogsPop );
		
		System.out.println("Mongo Posts: " + collPosts.getCount() );	
*/		
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
