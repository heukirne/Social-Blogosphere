import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class ExportMongoGraph {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection coll;
	
    public static void main(String[] args) throws Exception {		
	
        
		if (args.length!=1) {
		   System.out.println("Falta nome da colletion");
		   System.exit(1);
		}

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}
		
		String collName = args[0];
		coll = mongoDb.getCollection("pageRank_2"+collName);
		
		Writer output = null;
		File file = new File(collName+".graphml");
		output = new BufferedWriter(new FileWriter(file));  
		
		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<graph edgedefault=\"directed\">\n");

		DBCursor cur = coll.find();
        	while(cur.hasNext()) {
        		DBObject obj = cur.next();
			String idUser = obj.get("_id").toString().replaceAll("[\\W]","");

			BasicDBList listComments = (BasicDBList)((BasicDBObject)obj.get("value")).get("outL");
        		if (listComments.size()>0)
			for (Object id : listComments) {
				output.write("<edge source=\"" + idUser + "\" target=\"" + id.toString().replaceAll("[\\W]","")  + "\" />\n");
        		}
        	}

		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
		mongoConn.close();
    }

}

