import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;
import org.neo4j.helpers.collection.IteratorUtil;

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
 
	public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    public static final String DB_BLOG = "D:/xampplite/neo4j/data/graph.db";
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=root&password=";
	public static final String DB_BASE = "../base/neo4j";
	public static final String AUTHOR_KEY = "profileId";
	public static final String COMMENT_KEY = "link";
	public static final String TAG_KEY = "term";
	public static final String POST_KEY = "post";
	public static final String BLOG_KEY = "blog";
	public static final String PROP_KEY = "info";
    public static GraphDatabaseService graphDb;
	public static GraphDatabaseService readonlyDb;
    public static Index<Node> userIndex; 
	public static Index<Node> tagIndex; 
	public static Index<Node> postIndex;
	public static Index<Node> blogIndex;
	public static Index<Node> propertyIndex;
	public static Index<Relationship> commentIndex; 
	public static Node dummyNode;
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
		
		graphDb = new EmbeddedGraphDatabase( DB_BASE );
        userIndex = graphDb.index().forNodes( "authors" );
		tagIndex = graphDb.index().forNodes( "tags" );
		postIndex = graphDb.index().forNodes( "posts" );
		blogIndex = graphDb.index().forNodes( "blogs" );
		commentIndex = graphDb.index().forRelationships( "comments" );
		
		registerShutdownHook();
		
        int blogsTotal = 0;

		Transaction tx = graphDb.beginTx();
		try {	
			blogsTotal = blogIndex.query( BLOG_KEY , "*" ).size();
			System.out.println("Neo4j Users:" + userIndex.query( AUTHOR_KEY , "*" ).size() );
			System.out.println("Neo4j Blogs:" + blogsTotal);
			System.out.println("Neo4j Posts:" + postIndex.query( POST_KEY , "*" ).size());
			System.out.println("Neo4j Tags:" + tagIndex.query( TAG_KEY , "*" ).size());
			System.out.println("Neo4j Comments:" + commentIndex.query( COMMENT_KEY , "*" ).size());
			tx.success();
		} finally {
			tx.finish();
		}
		
		String mapBlogs =	"function(){ " +
				"	emit( this.blogID , this.comments.length ); "+
				"};";							
		
        String reduceAvg = "function( key , values ){ "+
			"	var totPosts = values.length; var totCom = 0; " +
			"	for ( var i=0; i<values.length; i++ ) {"+
			"		totCom += values[i]; "+
			"   } " +
			"	return { posts: totPosts, comments: totCom, avg: totCom/totPosts } ; "+
			"};";
		
        MapReduceOutput output = collPosts.mapReduce(mapBlogs, reduceAvg, "blogStats", MapReduceCommand.OutputType.REPLACE, null);
		DBCollection collResult = output.getOutputCollection();

		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = mpQuery.start("value.avg").greaterThanEquals(1).get();
	
        QueryBuilder query = new QueryBuilder();
        DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).get();

        long blogsActive = collResult.getCount();
        int blogsLive = collPosts.distinct("blogID", docQuery).size();
        int blogsLonely = (int)blogsActive - blogsLive;

		long blogsPop = collResult.getCount(mpDoc);
		int blogsInactive = blogsTotal - (int)blogsActive;
		
		String sql = "UPDATE blogStats SET " +
					"total = " + blogsTotal + "," +
					"active = " + blogsActive + "," +
					"inactive = " + blogsInactive + "," +
					"live = " + blogsLive + ", " +
					"popular = " + blogsPop + ", " +
					"lonely = " + blogsLonely + " LIMIT 1";	
		myStm.executeUpdate(sql);
		
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
        graphDb.shutdown();
		mongoConn.close();
		try {
			myStm.close();
		} catch (Exception ex) {}
    }
	
    private static void registerShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }
}