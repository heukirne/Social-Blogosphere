import com.mongodb.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class PageRank {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=myself&password=myself";
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

		try {
			mysqlConn = DriverManager.getConnection(myConnString);
			myStm = mysqlConn.createStatement();
			myStm.executeQuery("set wait_timeout = 7200");
		} catch (Exception e) {
			System.out.println("MySQL Offline.");
			System.exit(1);
		}
		
		collPosts = mongoDb.getCollection("posts");

		String mapAuthor =	"function(){ " +
							"		idAuthor = this.authorID;"+
							"		emit ( this.authorID, { post:1, comment:0, outL:[] } );"+
							"		this.comments.forEach ( function (comment) { "+
							"			if (comment.authorID!=idAuthor) {" +
							"				emit ( comment.authorID , { post:0, comment:1 , outL:[ idAuthor ] } ); " +
							"			}"+
							"		} ); "+
							"}; ";								

        String reduceAuthor = "function( key , values ){ "+
							"	var result = { post:0, comment:0, outL:[] }; " +
							"	values.forEach(function(value) {"+
							"		result.post += value.post;"+
							"               result.comment += value.comment;"+
							"		result.outL = result.outL.concat(value.outL); "+
							"   }); " +
							"	return result; "+
							"};";	
							

		String mapAuthor2 =	"function(){ " +
							"	var prK = this.value.pr/this.value.outL.length;"+
							"	this.value.outL.forEach ( function (value) { "+
							"		emit ( value , { pr:prK , outL:[] } ); " +
							"	} ); "+
							"	emit ( this._id , { pr: 0 , outL: this.value.outL  } );" +
							"}; ";	
							

        String reduceAuthor2 = "function( key , values ){ "+
							"	var result = { pr:0 , outL:[] }; " +
							"	values.forEach(function(value) {"+
							"		result.pr += value.pr; "+
							"		result.outL = result.outL.concat(value.outL); "+
							"   }); " +
							"	return result; "+
							"};";													
		
	QueryBuilder query = new QueryBuilder();
	DBObject docQuery = query.start("tags").is("politica").get();
	//docQuery = query.start("tags").is("politica").and("authorID").is("15379833583638166492").get();
	//docQuery = query.start("content").is(Pattern.compile("politica",Pattern.CASE_INSENSITIVE)).and("authorID").is("15379833583638166492").get();
	//docQuery = query.start("content").is(Pattern.compile("politica",Pattern.CASE_INSENSITIVE)).get();


    	//MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "pageRank_1", MapReduceCommand.OutputType.REPLACE ,docQuery);
 	DBCollection collResult = mongoDb.getCollection("pageRank_1");
/*
	DBCollection collAuthor = mongoDb.getCollection("authorAll");

	BasicDBObject doc = new BasicDBObject();
	Double prPost = 0d;
	Double prComment = 0d;
        DBCursor cur = collResult.find();
        while(cur.hasNext()) {
                BasicDBObject obj = (BasicDBObject)cur.next();

		doc = new BasicDBObject();
		doc.put("_id",obj.get("_id").toString());
		BasicDBObject author = (BasicDBObject)collAuthor.findOne(doc);
		BasicDBObject objValue = (BasicDBObject)obj.get("value");

		prPost = objValue.getDouble("post");
		prPost = (prPost * prPost) / ((BasicDBObject)author.get("value")).getDouble("post");
		prComment = objValue.getDouble("comment");
		prComment = (prComment * prComment) / ((BasicDBObject)author.get("value")).getDouble("comment");

		if (prPost.isNaN()) { prPost = 0d; }
		if (prComment.isNaN()) { prComment = 0d; }

		obj.put("pr_post",prPost);
		obj.put("pr_comment",prComment);
		objValue.put("pr",prPost + prComment * 0.33d);
		obj.append("value",objValue);

		collResult.save(obj);
        }
*/


	
    	MapReduceOutput output2 = collResult.mapReduce(mapAuthor2, reduceAuthor2, "pageRank_2", MapReduceCommand.OutputType.REPLACE, null);
	DBCollection collResult2 = output2.getOutputCollection();

	BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value.pr", -1);
        
        DBCursor cur = collResult2.find().sort(sortDoc).limit(10);
        int hash = checkSum(cur);
		int hash2 = 0;
		for (int i=0; i<10; i++) {
	        output2 = collResult2.mapReduce(mapAuthor2, reduceAuthor2, "pageRank_2", null);
			collResult2 = output2.getOutputCollection();

	        cur = collResult2.find().sort(sortDoc).limit(10);
	        hash2 = checkSum(cur);

	        //if (hash == hash2) break;
	        //else hash = hash2;
		}



	collResult = mongoDb.getCollection("pageRank_2");
        sortDoc = new BasicDBObject();
        sortDoc.put("value.pr", -1);

	cur = collResult.find().sort(sortDoc).limit(1000);
	String Interesses = "";
        while(cur.hasNext()) {
                DBObject obj = cur.next();
		
		myStm.executeQuery("SELECT Interesses, Introducao FROM author WHERE profileID = '" + obj.get("_id").toString() + "';");
		ResultSet rs = myStm.getResultSet();
		if (rs.next()) {
			if (rs.getString("Interesses") != null && rs.getString("Introducao") != null) {
				Interesses = rs.getString("Interesses") + rs.getString("Introducao");
				obj.put("Interesses",Interesses);
				//System.out.print(obj.get("_id").toString());
				//System.out.println(" > " + Interesses);
				collResult.save(obj);
			}
		}

        }


	
        mongoConn.close();
	try {
        	myStm.close();
        } catch (Exception ex) {}

    }

    private static int checkSum(DBCursor cur) {
        int sum = 0;
        while(cur.hasNext()) {
    		DBObject obj = cur.next();
    		for(char num: obj.get("_id").toString().toCharArray() ) 
    			sum += Character.getNumericValue(num);
    	}	
    	System.out.println(sum);
    	return sum;
    }

}
