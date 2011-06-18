import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

//import com.tinkerpop.blueprints.pgm.impls.neo4j.*;

import com.google.gdata.client.Query;
import com.google.gdata.client.blogger.BloggerService;
import com.google.gdata.data.*;
import com.google.gdata.util.ServiceException;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class AuthorIterate {
 
    private static final String DB_PATH = "var/base";
	private static final String AUTHOR_KEY = "profileId";
	private static final String COMMENT_KEY = "link";
	private static final String TAG_KEY = "term";
	private static final String POST_KEY = "post";
	private static final String BLOG_KEY = "blog";
    private static GraphDatabaseService graphDb;
    private static Index<Node> userIndex; 
	private static Index<Node> tagIndex; 
	private static Index<Node> postIndex;
	private static Index<Node> blogIndex;
	private static Index<Relationship> commentIndex; 
 
     private static enum RelTypes implements RelationshipType
    {
        Comments,
        Tags
    }
 
    public static void main(String[] args) throws Exception {
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        userIndex = graphDb.index().forNodes( "authors" );
		tagIndex = graphDb.index().forNodes( "tags" );
		postIndex = graphDb.index().forNodes( "posts" );
		blogIndex = graphDb.index().forNodes( "blogs" );
		commentIndex = graphDb.index().forRelationships( "comments" );
        registerShutdownHook();

		ArrayList<String> ArrBlogs = new ArrayList<String>();
		ArrBlogs.add("23198109");
		ArrBlogs.add("blogentrelinhas.blogspot.com");
		ArrBlogs.add("brasilempreende.blogspot.com");
		ArrBlogs.add("fetexas.blogspot.com");
		ArrBlogs.add("www.imprensabrasileira.com.br");
		ArrBlogs.add("premiobeijaflor.blogspot.com");
		ArrBlogs.add("imprensadobrasil.blogspot.com");
		ArrBlogs.add("tvfuzarca.blogspot.com");
		ArrBlogs.add("brasileducom.blogspot.com");
		ArrBlogs.add("capitao-obvio.blogspot.com");
		ArrBlogs.add("carrosjuizdefora.blogspot.com");
		ArrBlogs.add("blogdoparrini.blogspot.com");
		ArrBlogs.add("publicidadesportiva.blogspot.com");
		ArrBlogs.add("vocesabendomais.blogspot.com");
		ArrBlogs.add("caranovanocongresso.blogspot.com");
		ArrBlogs.add("bloggersemacao.blogspot.com");
		ArrBlogs.add("pelofimdosenado.blogspot.com");
		ArrBlogs.add("fuscabrasil.blogspot.com");
		ArrBlogs.add("verme-lho.blogspot.com");
		ArrBlogs.add("estrelapresidente.blogspot.com");
		ArrBlogs.add("brasildefacto.blogspot.com");
		ArrBlogs.add("carlosferreirajf.blogspot.com");

		
		
		for (String blogID : ArrBlogs) {
			Transaction tx = graphDb.beginTx();
			try {
				if (blogIndex.get( BLOG_KEY, blogID).size()==0) {
					if (getPosts(blogID)) {
						Node blog = graphDb.createNode();
						blog.setProperty( BLOG_KEY, blogID);
						blogIndex.add(blog, BLOG_KEY, blogID);
					} 
				}
				tx.success();
			} finally {
				tx.finish();
			}
		}
		
		System.out.println("Numbers of Users:" + userIndex.query( AUTHOR_KEY , "*" ).size() );
		System.out.println("Numbers of Blogs:" + blogIndex.query( BLOG_KEY , "*" ).size());
		System.out.println("Numbers of Tags:" + tagIndex.query( TAG_KEY , "*" ).size());
		System.out.println("Numbers of Posts:" + postIndex.query( POST_KEY , "*" ).size());
		System.out.println("Numbers of Comments:" + commentIndex.query( COMMENT_KEY , "*" ).size());

        System.out.println( "Shutting down database ..." );
        shutdown();
    }

	public static Boolean getPosts(final String blogUri) throws ServiceException, IOException {
			System.out.print("Retriving Posts from:" + blogUri);
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			Transaction tx = graphDb.beginTx();
			try {
				URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogUri + "/posts/default");
				if (!blogUri.matches("\\d+")) {
					feedUrl = new URL("http://" + blogUri + "/feeds/posts/default");
				} 
				Query myQuery = new Query(feedUrl);
				myQuery.setStartIndex(1);
				myQuery.setMaxResults(25);
				Feed resultFeed = myService.query(myQuery, Feed.class);
				
				Integer count = 0;
				for (Entry entry : resultFeed.getEntries()) {
					String linkID = entry.getLinks().get(2).getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");
					if (postIndex.get( POST_KEY, linkID).size()==0) {
						count++;
						System.out.print("," + count);
						ArrayList<Node> ArrNodes = getComments(linkID);
						if (ArrNodes.size()==0)
							throw new MalformedURLException();
						for ( Category category : entry.getCategories() ) {
							String text = Normalizer.normalize(category.getTerm(), Normalizer.Form.NFD);
							text = text.replaceAll("[^\\p{ASCII}]", "");
							Node tag = TagNode(text);
							for ( Node commentNode : ArrNodes )
								TagRelation(tag, commentNode);
						}
						Node post = graphDb.createNode();
						post.setProperty( POST_KEY, linkID);
						postIndex.add(post, POST_KEY, linkID);
					}
				}
				System.out.print("\n");
				
				tx.success();
				return true;
			} catch (MalformedURLException e) {
				System.out.println("Malformed URL Exception!");
				return false;
			} catch (IOException e) {
				System.out.println("IOException!");
				return false;
			} catch (ServiceException e) {
				System.out.println("Service Exception!");
				return false;
			} finally {
				tx.finish();
			}
	}	
	
	public static ArrayList<Node> getComments(final String postUri) throws ServiceException, IOException {
			System.out.print(" " + postUri + ",");
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			ArrayList<Node> ArrNodes = new ArrayList<Node>();
			Transaction tx = graphDb.beginTx();
			try {
				URL feedUrl = new URL("http://www.blogger.com/feeds/" + postUri + "/comments/default");
				Query myQuery = new Query(feedUrl);
				myQuery.setStartIndex(1);
				myQuery.setMaxResults(25);
				Feed resultFeed = myService.query(myQuery, Feed.class);
				
				String authorID = resultFeed.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
				Node authorNode = createAuthorNode(authorID);
				ArrNodes.add(authorNode);
				for (Entry entry : resultFeed.getEntries())
				{
					if (entry.getAuthors().get(0).getUri()!=null) {
						String profileID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
						if (profileID.matches("\\d+")) {
							String linkID = entry.getLinks().get(0).getHref().replace("http://www.blogger.com/feeds/","");
							Node commentNode = createAuthorNode(profileID);
							ArrNodes.add(commentNode);
							CommentRelation(commentNode, authorNode, linkID);
						}
					}
				}
				tx.success();
			} catch (MalformedURLException e) {
				System.out.println("Malformed URL Exception!");
			} catch (IOException e) {
				System.out.println("IOException!");
			} catch (ServiceException e) {
				System.out.println("Service Exception!");
			} finally {
				tx.finish();
				return ArrNodes;
			}
	}
	
    private static Node createAuthorNode( final String profileID )
    {
        if (userIndex.get( AUTHOR_KEY, profileID).size()==0) {
			Node node = graphDb.createNode();
			node.setProperty( AUTHOR_KEY, profileID);
			userIndex.add( node, AUTHOR_KEY, profileID);
			return node;
		} else {
			return userIndex.get( AUTHOR_KEY, profileID).getSingle();
		}
    }	
	
    private static Node TagNode( final String Term )
    {
        if (tagIndex.get( TAG_KEY, Term).size()==0) {
			Node node = graphDb.createNode();
			node.setProperty( TAG_KEY, Term);
			tagIndex.add( node, TAG_KEY, Term);
			return node;
		} else {
			return tagIndex.get( TAG_KEY, Term).getSingle();
		}
    }	
	
    private static void TagRelation( final Node TagNode, final Node AuthorNode)
    {
		//System.out.println("+ " + TagNode.getProperty(TAG_KEY) );
		Boolean hasRelation = false;
		for ( Relationship relationship : AuthorNode.getRelationships( RelTypes.Tags, Direction.OUTGOING ) )
			if (hasRelation = relationship.getEndNode().equals(TagNode)) {
				int Weight = ((Integer)relationship.getProperty("Weight")).intValue();
				relationship.setProperty("Weight", Weight+1);
			}
		if (!hasRelation) {
			Relationship relationship = AuthorNode.createRelationshipTo(TagNode, RelTypes.Tags);
			relationship.setProperty("Weight", 1);
		}
    }
	
    private static void CommentRelation( final Node CommentNode, final Node AuthorNode, final String Uri )
    {
		//System.out.println("+ " + CommentNode.getProperty(AUTHOR_KEY) + "\t" + Uri );
        if (!CommentNode.equals(AuthorNode) && commentIndex.get( COMMENT_KEY, Uri).size()==0) {
			Boolean hasRelation = false;
			for ( Relationship relationship : CommentNode.getRelationships( RelTypes.Comments, Direction.OUTGOING ) )
                if (hasRelation = relationship.getEndNode().equals(AuthorNode)) {
					int Weight = ((Integer)relationship.getProperty("Weight")).intValue();
					ArrayList<String> links = new ArrayList<String>(Arrays.asList((String[]) relationship.getProperty("links")));
					links.add(Uri);
					relationship.setProperty("Weight", Weight+1);
					relationship.setProperty("links", links.toArray(new String[links.size()]));
					commentIndex.add(relationship , COMMENT_KEY, Uri);
				}
			if (!hasRelation) {
				Relationship relationship = CommentNode.createRelationshipTo(AuthorNode, RelTypes.Comments);
				relationship.setProperty("Weight", 1);
				ArrayList<String> links = new ArrayList<String>();
				links.add(Uri);
				relationship.setProperty("links", links.toArray(new String[links.size()]));
				commentIndex.add(relationship , COMMENT_KEY, Uri);
			}
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
