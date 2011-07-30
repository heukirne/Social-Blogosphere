import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;

import java.sql.*;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
/**
 Normalize Table
 update author set find = 1 where length(blogs) > 2 and find = 0;
 update author set find = 1 where length(concat(Local,Atividade,sexo)) > 5 and find = 0;
 delete from author where length(profileId) < 20;
 */
public class Author2MySQL {
 
    private static final String DB_BASE = "base/neo4j";
	private static final String myConnString = "jdbc:mysql://localhost/gemeos110?user=gemeos110&password=dias09ufrgs";
    private static GraphDatabaseService graphDb;
    private static Index<Node> userIndex; 
	private static Connection mysqlConn;

      private static enum RelTypes implements RelationshipType
    {
		Comments
    }
 
 	 public static int relationSize(Iterable<Relationship> iterable) {
		int cont = 0;
		for (Relationship relation : iterable) cont++;
		return cont;
	  }
 
    public static void main(String[] args) throws Exception {		
		
		mysqlConn = DriverManager.getConnection(myConnString);
		Statement myStm = mysqlConn.createStatement();
			
		graphDb = new EmbeddedReadOnlyGraphDatabase( DB_BASE );
		userIndex = graphDb.index().forNodes( "authors" );
		registerShutdownHook();
		
		int cnt = 1;
		for ( Node node : userIndex.query("profileId" , "*") ) {
			//System.out.print( node.getProperty("profileId") );
			if(cnt++>0) {
				try {
					myStm.executeUpdate("INSERT INTO author SET profileID = '" + node.getProperty("profileId") + "'");	
					System.out.print( "+" );
				} catch (SQLException ex) {	
					System.out.print( "-" );
				} 
				finally { 
					int size = relationSize(node.getRelationships(RelTypes.Comments, Direction.BOTH));
					myStm.executeUpdate("UPDATE author SET "
						+ " Degree = " + size + ""
						+ " WHERE profileID = '" + node.getProperty("profileId") + "'");						
				}
			}
			//break;
		}
		
        System.out.println( "Shutting down database ..." );
        shutdown();
    }	
	
    private static void shutdown()
    {
        graphDb.shutdown();
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
