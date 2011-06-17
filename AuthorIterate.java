import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import com.google.gdata.client.Query;
import com.google.gdata.client.blogger.BloggerService;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Entry;
import com.google.gdata.data.Feed;
import com.google.gdata.data.Person;
import com.google.gdata.data.PlainTextConstruct;
import com.google.gdata.data.TextContent;
import com.google.gdata.util.ServiceException;

import java.net.*;
import java.io.*;
import javax.xml.parsers.*;
import org.xml.sax.helpers.DefaultHandler;
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class AuthorIterate {
 
    private static final String DB_PATH = "var/base";
	private static final String AUTHOR_KEY = "id";
	private static final String COMMENT_KEY = "link";
    private static GraphDatabaseService graphDb;
    private static Index<Node> indexService; 
	private static Index<Relationship> relationService; 
 
     private static enum RelTypes implements RelationshipType
    {
        Comments,
        Tags,
		Property
    }
 
    public static void main(String[] args) throws Exception {
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        indexService = graphDb.index().forNodes( "authors" );
		relationService = graphDb.index().forRelationships( "comments" );
        registerShutdownHook();
 
 
        Transaction tx = graphDb.beginTx();
        try {
			
			
			//http://code.google.com/intl/pt-BR/apis/blogger/docs/2.0/developers_guide_java.html
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			
			URL feedUrl = new URL("http://www.blogger.com/feeds/23198109/comments/default");
			//Feed resultFeed = myService.getFeed(feedUrl, Feed.class);
			Query myQuery = new Query(feedUrl);
			myQuery.setStartIndex(1);
			myQuery.setMaxResults(50);
			Feed resultFeed = myService.query(myQuery, Feed.class);
			
			
			String authorID = resultFeed.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			System.out.println("Author: " + authorID );
			Node authorNode = createAndIndexNode(authorID);
			// Print the results
			for (int i = 0; i < resultFeed.getEntries().size(); i++) {
				Entry entry = resultFeed.getEntries().get(i);
				if (entry.getAuthors().get(0).getUri()!=null) {
					String profileID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
					String linkID = entry.getLinks().get(0).getHref().replace("http://www.blogger.com/feeds/","");
					Node commentNode = createAndIndexNode(profileID);
					createRelationship(commentNode, authorNode, linkID);
					System.out.println("+ " + profileID + "\t" + linkID );
				}
			}
			
            for ( Node hit : indexService.query( AUTHOR_KEY , "*" ) )
            {
				if (hit.hasProperty(AUTHOR_KEY))
					System.out.print("Node:" + hit.getId() + " is " + hit.getProperty(AUTHOR_KEY) + "\n");
            }	
			
            for ( Relationship relationship : authorNode.getRelationships(
                    RelTypes.Comments, Direction.INCOMING ) )
            {
                Node hit = relationship.getStartNode();
				if (hit.hasProperty(AUTHOR_KEY))
					System.out.print("Relation:" + hit.getProperty(AUTHOR_KEY) + " is " + relationship.getProperty(COMMENT_KEY) + "\n");
            }
			
			tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "Shutting down database ..." );
        shutdown();
    }

    private static Node createAndIndexNode( final String profileID )
    {
        if (indexService.get( AUTHOR_KEY, profileID).size()==0) {
			Node node = graphDb.createNode();
			node.setProperty( AUTHOR_KEY, profileID);
			indexService.add( node, AUTHOR_KEY, profileID);
			return node;
		} else {
			return indexService.get( AUTHOR_KEY, profileID).getSingle();
		}
    }	
	
    private static void createRelationship( final Node CommentNode, final Node AuthorNode, final String Uri )
    {
        if (!CommentNode.equals(AuthorNode) && relationService.get( COMMENT_KEY, Uri).size()==0) {
			Relationship relationship = CommentNode.createRelationshipTo(AuthorNode, RelTypes.Comments);
			relationship.setProperty(COMMENT_KEY, Uri);
			relationService.add(relationship , COMMENT_KEY, Uri);
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
