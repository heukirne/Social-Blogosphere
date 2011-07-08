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
 
    private static final String DB_BLOG = "D:/xampplite/neo4j/data/graph.db";
	private static final String myConnString = "jdbc:mysql://localhost/blogger?user=root&password=";
    private static GraphDatabaseService graphDb;
    private static Index<Node> userIndex; 
	private static Index<Node> propertyIndex;
	private static Connection mysqlConn;

      private static enum RelTypes implements RelationshipType
    {
		Property
    }
 
    public static void main(String[] args) throws Exception {		
		
		mysqlConn = DriverManager.getConnection(myConnString);
		Statement myStm = mysqlConn.createStatement();
			
		String propertiList = " Local,Atividade,Sexo,Signo_astrologico,Profissao,";
		graphDb = new EmbeddedReadOnlyGraphDatabase( DB_BLOG );
		Index<Node> propertyIndex = graphDb.index().forNodes( "property" );
		Node brNode = propertyIndex.query( "info" , "BR" ).getSingle();
		registerShutdownHook();
		
		int cnt = 0;
		ArrayList<String> blogsAr = new ArrayList<String>();
		for ( Relationship relationBR : brNode.getRelationships( RelTypes.Property, Direction.INCOMING ) ) {
			Node node = relationBR.getStartNode();	
			if (node.hasProperty("id") && !node.hasProperty("blogs") && !node.hasProperty("blogs")) {
				if(cnt++>0) {
					try {
						myStm.executeUpdate("INSERT INTO author SET profileID = '" + node.getProperty("id") + "', Local = 'BR'");	

						blogsAr = new ArrayList<String>();
						if (node.hasProperty("blogs"))
							blogsAr = new ArrayList<String>(Arrays.asList((String[]) node.getProperty("blogs")));
						myStm.executeUpdate("UPDATE author SET "
							+ "Local = '" + node.getProperty("Local","") + "',"
							+ "Atividade = '" + node.getProperty("Atividade","") + "',"
							+ "Sexo = '" + node.getProperty("Sexo","") + "',"
							+ "Signo_astrologico = '" + node.getProperty("Signo_astrologico","") + "',"
							+ "Profissao = SUBSTRING('" + node.getProperty("Profissao","") + "',200),"
							+ "Blogs = '" + blogsAr.toString() + "'"
							+ " WHERE profileID = '" + node.getProperty("id") + "'");						
						System.out.println( "" );
						System.out.print( cnt + ", " + node.getProperty("id") );
						System.out.println( "+" );
					} catch (SQLException ex) {	
						System.out.print( "-" );
					} 
					finally { }
				}
			}
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
