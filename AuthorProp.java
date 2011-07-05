import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class AuthorProp {
 
    private static final String DB_BLOG = "D:/xampplite/neo4j/data/graph.db";
	private static final String PROP_KEY = "info";
	private static final String AUTHOR_KEY = "id";
    private static GraphDatabaseService graphDb;
    private static Index<Node> userIndex; 
	private static Index<Node> propertyIndex;

 
     private static enum RelTypes implements RelationshipType
    {
        Comments,
        Tags,
		Property
    }

    public static void main(String[] args) throws Exception {		
		
		String propertiList = " Local,Atividade,Sexo,Signo_astrologico,Profissao,";
		graphDb = new EmbeddedGraphDatabase( DB_BLOG );
        userIndex = graphDb.index().forNodes( "authors" );
		propertyIndex = graphDb.index().forNodes( "property" );
		registerShutdownHook();
		
		Transaction tx = graphDb.beginTx();
		Boolean bInfo = false;
		int count = 0;
		try {
			Node nodeBR = propertyIndex.get( PROP_KEY, "BR").getSingle();
			for ( Relationship relationship : nodeBR.getRelationships( RelTypes.Property, Direction.INCOMING ) ) {
				Node author = relationship.getStartNode();
				for ( String key : author.getPropertyKeys() ) {
					if (propertiList.indexOf(key+",")>0) {
						PropertyRelation(author.getProperty(key).toString(), author);
						bInfo = true;
					}
				}
				if (bInfo) { System.out.println(author.getId()); count++;  bInfo=false;}
				break;
			}

			tx.success();
		} finally {
			tx.finish();
		}
		
        System.out.println( "Shutting down database ..." );
        shutdown();
    }	
	
    private static void PropertyRelation( final String Property, final Node AuthorNode)
    {
		Node PropertyNode =  propertyIndex.get( PROP_KEY, Property).getSingle();
        if (PropertyNode==null) {
			PropertyNode = graphDb.createNode();
			PropertyNode.setProperty( PROP_KEY, Property);
			propertyIndex.add( PropertyNode, PROP_KEY, Property);
		}
	
		Boolean hasRelation = false;
		for ( Relationship relationship : AuthorNode.getRelationships( RelTypes.Property, Direction.OUTGOING ) )
			if (relationship.getEndNode().equals(PropertyNode))
				hasRelation = true;
			
		if (!hasRelation) {
			Relationship relationship = AuthorNode.createRelationshipTo(PropertyNode, RelTypes.Property);
		}
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
