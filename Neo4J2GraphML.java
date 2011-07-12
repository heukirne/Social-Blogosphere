import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class Neo4J2GraphML {
 
    private static final String DB_BASE = "base/neo4j";
	private static Index<Node> userIndex; 
    private static GraphDatabaseService graphDb;

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
				
		graphDb = new EmbeddedReadOnlyGraphDatabase( DB_BASE );
		userIndex = graphDb.index().forNodes( "authors" );
		registerShutdownHook();
		
		Writer output = null;
		File file = new File("neo4j.graphml");
		output = new BufferedWriter(new FileWriter(file));  
		
		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<key attr.name=\"weight\" attr.type=\"double\" for=\"edge\" id=\"weight\"/>\n");
		output.write("<graph edgedefault=\"directed\">\n");
		
		for ( Node node : userIndex.query("profileId" , "*") ) {
			output.write("<node id=\"" + node.getId() + "\" />\n");
			for (Relationship rel : node.getRelationships(RelTypes.Comments, Direction.INCOMING)) {
				output.write("<edge source=\"" + rel.getStartNode().getId() + "\" target=\"" + rel.getEndNode().getId() + "\">\n");
				if (rel.hasProperty("weight"))
					output.write("<data key=\"weight\">" + rel.getProperty("weight") + "</data>\n");
				else
					output.write("<data key=\"weight\">1.0</data>\n");
				output.write("</edge>\n");
			}
			//break;
		}
		
		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
		System.out.println( "Arquivo neo4j.graphml gerado!" );
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
