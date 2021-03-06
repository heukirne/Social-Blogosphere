import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class MongoDBTry {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	private static Map<String, MutableInteger> authorDegree;
	
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
		//collPosts.ensureIndex("postID");
		//collPosts.ensureIndex("blogID");				
		//collPosts.ensureIndex("authorID");

		//registerShutdownHook();
		
		String mapTags = 	"function(){ "+
							"	this.tags.forEach( "+
							"		function(tag){ "+
							"			emit( tag , { count : 1 } ); "+
							"		} "+
							"	); "+
							"};";
		
		String mapMonth = 	"function(){ "+
							"	var d = new Date(this.published); "+
							"	emit( d.getMonth() , { count : 1 } ); "+
							"};";
							
		String mapContent =	"function(){ " +
							"   if (this.content)" +
							"	this.content.split(' ').forEach( " +
							"		function(word){ " +
							"			word.split('\\n').forEach( " +
							"				function(piece){ emit( piece , 1 ); } "+
							"			); "+
							"		} "+
							"	); "+
							"};";	

		String mapAuthor =	"function(){ " +
							"idAuthor = this.authorID;"+
							"this.comments.forEach ( function (comment) {"+ 
								"if (comment.authorID != idAuthor) {"+
								"emit (comment.authorID, { post : 0 , comment : 1   } ); } "+
							"} );"+
							"emit( this.authorID , { post : 1, comment : 0 } ); "+
							"};";							
		
        String reduceAvg = "function( key , values ){ "+
							"	var posts = 0; var totCom = 0; " +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		posts += values[i].count; "+
							"		totCom += values[i].comments; "+
							"   } " +
							"	return Math.round(totCom/posts); "+
							"};";
							
        String reduceAuthor = "function( key , values ){ "+
							"	var Tcomment = 0; var Tpost = 0; " +
							"	values.forEach(function(value) {"+
							"		Tpost += value.post; "+
							"		Tcomment += value.comment;"+
							"   }); " +
							"	return { post : Tpost , comment : Tcomment }; "+
							"};";						
		

		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("tags").is("politica").get();		
		
		
        MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "authorPag1", MapReduceCommand.OutputType.REPLACE, null);
		//DBCollection collResult = output.getOutputCollection();
		

		//MapReduceOutput output = collPosts.mapReduce(mapContent, reduceAuthor, "words", MapReduceCommand.OutputType.REPLACE, docQuery);

		/*
		DBCollection collResult = mongoDb.getCollection("atuhorComp");

		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = query.start("value").notEquals(new BasicDBList()).get();
		
		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value", -1);
        */


        /*
		Writer output = null;
		File file = new File("wordCommunity.graphml");
		output = new BufferedWriter(new FileWriter(file));  
		
		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<key attr.name=\"network\" attr.type=\"int\" for=\"node\" id=\"network\"/>\n");
		output.write("<graph edgedefault=\"directed\">\n");

		authorDegree = new HashMap<String, MutableInteger>();

		int contCur = 0;
		String[] arComments = null;
		DBCursor cur = collResult.find().sort(sortDoc);
        while(cur.hasNext()) {
        	DBObject obj = cur.next();
        	arComments = obj.get("value").toString().split(",");

        	for (String id : arComments) {
        		if (!obj.get("_id").toString().equals(id)) {
        			output.write("<edge source=\"" + id + "\" target=\"" + obj.get("_id").toString() + "\" />\n");
        			incAuthor(id);
        			incAuthor(obj.get("_id").toString());
        		}
        	}
        }

		cur = collResult.find().sort(sortDoc);
        while(cur.hasNext()) {
        	DBObject obj = cur.next();
        	if (authorDegree.containsKey(obj.get("_id").toString())) {
				output.write("<node id=\"" + obj.get("_id").toString() + "\" >\n");
				output.write("<data key=\"network\">" + authorDegree.get(obj.get("_id").toString()).intValue() + "</data>\n");
				output.write("</node>\n");	
			}
		}

		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		*/
		
        shutdown();
    }

    private static void incAuthor(String id) {
        MutableInteger value = authorDegree.get(id);
        if (value == null) {    // Create new entry with count of 1.
            authorDegree.put(id, new MutableInteger(1));
        } else {                // Increment existing count by 1.
            value.inc();
        }
    }

    private static void shutdown()
    {
		System.out.println( "Shutting down database ..." );
		mongoConn.close();
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

class MutableInteger {
    private int m_value;
    
    /** Constructor */
    public MutableInteger(int value) {
        m_value = value;
    }
    
    /** Return int value. */
    public int intValue() {
        return m_value;
    }
    
    /** Increment value */
    public void inc() {
        m_value++;
    }
}
