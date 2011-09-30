import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;
import org.neo4j.helpers.collection.IteratorUtil;

import com.mongodb.*;

import com.google.gdata.client.Query;
import com.google.gdata.client.blogger.BloggerService;
import com.google.gdata.data.*;
import com.google.gdata.util.ServiceException;

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
 
public class AuthorIterate {
 
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
 
    public static enum RelTypes implements RelationshipType
    {
        Comments, Tags, Property
    }
	
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
		
		mongoConn.close();

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
		
		getBlogs();
		
        shutdown();
    }
	
	public static void getBlogs() throws Exception 
	{

		final int numCrawler = 1;
		Crawler[] crawler = new Crawler[numCrawler];

		ResultSet rs = null;
		String[] blogs = null;
		String profileID = "";
		int j = 1;
		int contCrawler = 0;

		while(true)
		{
			blogs = null;
			myStm.executeQuery("SELECT blogs, profileID FROM author WHERE Local = 'BR' and length(Blogs)>2 AND Find=1 AND retrieve=0 ORDER BY degree DESC LIMIT " + (contCrawler+1) + ",1");
			rs = myStm.getResultSet();
			if (rs!=null) {
				rs.next();
				blogs = Pattern.compile(",").split(rs.getString("blogs"));
				profileID = rs.getString("profileID");
			} else {
				break;
			}
		
			if (blogs==null) continue;

			//Better implement a BlockingQueue or Thread Pool Pattern
			crawler[contCrawler] = new Crawler(profileID, blogs);
			crawler[contCrawler].start();
			contCrawler++;

			if (contCrawler >= crawler.length) {
				for (int i=0; i<contCrawler; i++) 
					crawler[i].join();
				contCrawler = 0;
			}
			
			if ((j%10)==0) {
				Transaction tx = graphDb.beginTx();	
				String sql = "UPDATE neo4jstats SET " +
							"users = " + userIndex.query( AUTHOR_KEY , "*" ).size() + "," +
							"blogs = " + blogIndex.query( BLOG_KEY , "*" ).size() + "," +
							"tags = " + tagIndex.query( TAG_KEY , "*" ).size() + "," +
							"posts = " + postIndex.query( POST_KEY , "*" ).size() + ", " +
							"comments = " + commentIndex.query( COMMENT_KEY , "*" ).size() + " LIMIT 1";	
				tx.success();
				tx.finish();
				myStm.executeUpdate(sql);
				if (isExit()) break;
			}
			
			j++;
		}
		
	}
	
	private static Boolean isExit() {
		File file = new File("AuthorIterateExit.txt");
		StringBuffer contents = new StringBuffer();
		BufferedReader reader = null;
		
		 
		try {
			String text = null;
			reader = new BufferedReader(new FileReader(file));
			 
			while ((text = reader.readLine()) != null) {
				contents.append(text);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null) {
				reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		int intExit = Integer.parseInt(contents.toString());
		if (intExit==1)
			return true;
		else
			return false;
	}
	
    private static void shutdown()
    {
		System.out.println( "Shutting down database ..." );
        graphDb.shutdown();
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

class Crawler extends Thread {

	private String[] blogs;
	private String profileID;

	private Mongo mongoConn;
	private DB mongoDb;
	private DBCollection collPosts;	

    Crawler(final String profileID, final String[] blogs) throws Exception {
    	this.profileID = profileID;
        this.blogs = blogs;
    }
    public void run() {
    	try {
		
			mongoConn = new Mongo( "localhost" , 27017 );
			mongoDb = mongoConn.getDB( "blogdb" );
			collPosts = mongoDb.getCollection("posts");

    		for (String blog : this.blogs)
			{
				Transaction tx = AuthorIterate.graphDb.beginTx();
				try {
					String blogID = blog.trim().replace("http:","").replace("/","");
					if (AuthorIterate.blogIndex.get( AuthorIterate.BLOG_KEY, blogID).size()==0) {
						if (getPosts(blogID)) {
							//Blog Index only to store visited blogs
							Node blogNode = AuthorIterate.graphDb.createNode();
							blogNode.setProperty( AuthorIterate.BLOG_KEY, blogID);
							AuthorIterate.blogIndex.add(blogNode, AuthorIterate.BLOG_KEY, blogID);
						} 
					}
					tx.success();
				} finally {
					tx.finish();
				}
			}
			AuthorIterate.myStm.executeUpdate("UPDATE author SET retrieve = 1 WHERE profileID = '" + this.profileID + "' LIMIT 1");
		
		} catch (Exception ex) {
		} finally {
			mongoConn.close();
		}
		
    }

	private Boolean getPosts(final String blogUri) throws ServiceException, IOException {
			
			System.out.print("Retriving:" + blogUri);			
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			Transaction tx = AuthorIterate.graphDb.beginTx();
			
			try {
				URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogUri + "/posts/default");
				if (!blogUri.matches("\\d+")) {
					feedUrl = new URL("http://" + blogUri + "/feeds/posts/default");
				} 
				Query myQuery = new Query(feedUrl);
				//myQuery.setStartIndex(1);
				DateTime dtMin = DateTime.parseDate("2011-01-01");
				myQuery.setPublishedMin(dtMin);
				myQuery.setMaxResults(25);
				Feed resultFeed = myService.query(myQuery, Feed.class);
				
				String blogID = resultFeed.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("/posts/default/?published-min=2011-01-01","");					
				
				Integer count = 0;
				for (Entry entry : resultFeed.getEntries()) {
					String postID = "";
					try {
						postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
					} catch (Exception e) {
						break;
					}
					if (entry.getAuthors().get(0).getUri()!=null && AuthorIterate.postIndex.get( AuthorIterate.POST_KEY, postID).size()==0) {
					
						setMongoPost(entry);
						count++; System.out.print("," + count);	
						
						ArrayList<Node> ArrNodes = getComments(postID);
						for ( Category category : entry.getCategories() ) {
							String text = Normalizer.normalize(category.getTerm(), Normalizer.Form.NFD);
							text = text.replaceAll("[^\\p{ASCII}]", "");
							Node tag = TagNode(text);
							for ( Node commentNode : ArrNodes )
								TagRelation(tag, commentNode);
						}
						//Post Index only to store visited posts
						Node postNode = AuthorIterate.graphDb.createNode();
						postNode.setProperty( AuthorIterate.POST_KEY, postID);
						AuthorIterate.postIndex.add(postNode, AuthorIterate.POST_KEY, postID);
					}
				}
				
				//Get last 25 blog comments
				//System.out.println("blogID:"+blogID);
				//ArrayList<Node> ArrNodes = getComments(blogID); 
				
				System.out.print("\n");
				
				tx.success();
				return true;
			} catch (MalformedURLException e) {
				System.out.println("Malformed URL Exception!");
				return false;
			} catch (IOException e) {
				System.out.println("IOException!");
				return true;
			} catch (ServiceException e) {
				System.out.println("Service Exception!");
				return true;
			} catch (Exception e) {
				System.out.println("Error:" + e.getMessage());
				return true;
			} finally {
				tx.finish();
			}
	}
	
	private ArrayList<Node> getComments(final String postUri) throws ServiceException, IOException {
			//System.out.print(" " + postUri + ",");
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			ArrayList<Node> ArrNodes = new ArrayList<Node>();
			Transaction tx = AuthorIterate.graphDb.beginTx();
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
						//TODO
						String profileID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
						if (profileID.matches("\\d+")) {
							String commentID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("comments/default/","");
							Node commentNode = createAuthorNode(profileID);
							ArrNodes.add(commentNode);
							CommentRelation(commentNode, authorNode, commentID);
							setMongoComment(postUri, entry);
							updateDegree(commentNode);
						}
					}
				}
				updateDegree(authorNode);
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
	
	private void setMongoPost(Entry entry) {
		
		BasicDBObject doc = new BasicDBObject();

		String postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
		String blogID = postID.substring(0,postID.indexOf("/"));
		postID = postID.replace(blogID+"/","");
		
		doc.put("blogID", blogID);
		doc.put("postID", postID);
		
		if (collPosts.find(doc).count()==0) {
			String authorID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			doc.put("authorID", authorID);
			
			Date published = new Date(entry.getPublished().getValue());
			doc.put("published", published);
			
			String title = Normalizer.normalize(entry.getTitle().getPlainText(), Normalizer.Form.NFD);
			title = title.replaceAll("[^\\p{ASCII}]", "");
			doc.put("title", title);
			
			if (entry.getContent()!=null) {
				String content = Normalizer.normalize(((TextContent) entry.getContent()).getContent().getPlainText(), Normalizer.Form.NFD);
				content = content.replaceAll("[^\\p{ASCII}]", "");
				doc.put("content", content);
			}

			BasicDBList tags = new BasicDBList();
			for ( Category category : entry.getCategories() ) {
				String text = Normalizer.normalize(category.getTerm(), Normalizer.Form.NFD);
				text = text.replaceAll("[^\\p{ASCII}]", "");
				tags.add(tags.size(), text);
			}
			doc.put("tags", tags);
			doc.put("comments", new BasicDBList());

			collPosts.insert(doc);
		}
		
	}
	
	private void setMongoComment(String postUri, Entry entry) {

		BasicDBObject doc = new BasicDBObject();
		
		String blogID = postUri.substring(0,postUri.indexOf("/"));
		postUri = postUri.replace(blogID+"/","");
		
		doc.put("blogID", blogID);		
		doc.put("postID", postUri);
		
		if (collPosts.find(doc).count()!=0) {
			
			BasicDBObject post = (BasicDBObject)collPosts.findOne(doc);
		
			BasicDBList comments = (BasicDBList)post.get("comments");
			
			BasicDBObject comment = new BasicDBObject();
			
			String commentID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("comments/default/","");
			comment.put("commentID", commentID);
			
			String authorID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			comment.put("authorID", authorID);
			
			Date published = new Date(entry.getPublished().getValue());
			comment.put("published", published);
			
			String content = Normalizer.normalize(((TextContent) entry.getContent()).getContent().getPlainText(), Normalizer.Form.NFD);
			content = content.replaceAll("[^\\p{ASCII}]", "");
			comment.put("content", content);
			
			comments.put(comments.size(),comment);
	
			post.append("comments", comments);
		
			collPosts.save(post);
		}
	
	}
	
    private Node createAuthorNode( final String profileID ) throws Exception
    {
        if (AuthorIterate.userIndex.get( AuthorIterate.AUTHOR_KEY, profileID).size()==0) {
			Node node = AuthorIterate.graphDb.createNode();
			node.setProperty( AuthorIterate.AUTHOR_KEY, profileID);
			AuthorIterate.userIndex.add( node, AuthorIterate.AUTHOR_KEY, profileID);
			return node;
		} else {
			return AuthorIterate.userIndex.get( AuthorIterate.AUTHOR_KEY, profileID).getSingle();
		}
    }	
	
	private void updateDegree(Node node) throws Exception
	{
		if (node.hasProperty(AuthorIterate.AUTHOR_KEY)) {
			AuthorIterate.myStm.executeUpdate("INSERT INTO author SET profileID = '" + node.getProperty(AuthorIterate.AUTHOR_KEY) + "'");
			int size = IteratorUtil.count(node.getRelationships(AuthorIterate.RelTypes.Comments, Direction.BOTH));
			AuthorIterate.myStm.executeUpdate("UPDATE author SET degree = " + size + " WHERE profileID = '" + node.getProperty(AuthorIterate.AUTHOR_KEY) + "' LIMIT 1");
		}
	}
	
    private Node TagNode( final String Term )
    {
        if (AuthorIterate.tagIndex.get( AuthorIterate.TAG_KEY, Term).size()==0) {
			Node node = AuthorIterate.graphDb.createNode();
			node.setProperty( AuthorIterate.TAG_KEY, Term);
			AuthorIterate.tagIndex.add( node, AuthorIterate.TAG_KEY, Term);
			return node;
		} else {
			return AuthorIterate.tagIndex.get( AuthorIterate.TAG_KEY, Term).getSingle();
		}
    }	
	
    private void TagRelation( final Node TagNode, final Node AuthorNode)
    {
		//System.out.println("+ " + TagNode.getProperty(TAG_KEY) );
		Boolean hasRelation = false;
		for ( Relationship relationship : AuthorNode.getRelationships( AuthorIterate.RelTypes.Tags, Direction.OUTGOING ) )
			if (hasRelation = relationship.getEndNode().equals(TagNode)) {
				double Weight = Double.parseDouble(relationship.getProperty("Weight").toString());
				relationship.setProperty("Weight", Weight+1d);
			}
		if (!hasRelation) {
			Relationship relationship = AuthorNode.createRelationshipTo(TagNode, AuthorIterate.RelTypes.Tags);
			relationship.setProperty("Weight", 1d);
		}
    }
	
    private void CommentRelation( final Node CommentNode, final Node AuthorNode, final String Uri )
    {
		//System.out.println("+ " + CommentNode.getProperty(AUTHOR_KEY) + "\t" + Uri );
        if (!CommentNode.equals(AuthorNode) && AuthorIterate.commentIndex.get( AuthorIterate.COMMENT_KEY, Uri).size()==0) {
			Boolean hasRelation = false;
			for ( Relationship relationship : CommentNode.getRelationships( AuthorIterate.RelTypes.Comments, Direction.OUTGOING ) )
                if (hasRelation = relationship.getEndNode().equals(AuthorNode)) {
					double Weight = Double.parseDouble(relationship.getProperty("Weight").toString());
					relationship.setProperty("Weight", Weight+1d);
					AuthorIterate.commentIndex.add(relationship , AuthorIterate.COMMENT_KEY, Uri);
				}
			if (!hasRelation) {
				Relationship relationship = CommentNode.createRelationshipTo(AuthorNode, AuthorIterate.RelTypes.Comments);
				relationship.setProperty("Weight", 1d);
				AuthorIterate.commentIndex.add(relationship , AuthorIterate.COMMENT_KEY, Uri);
			}
		}
    }	
	
    private void PropertyRelation( final String Property, final Node AuthorNode)
    {
		Node PropertyNode =  AuthorIterate.propertyIndex.get( AuthorIterate.PROP_KEY, Property).getSingle();
        if (PropertyNode==null) {
			PropertyNode = AuthorIterate.graphDb.createNode();
			PropertyNode.setProperty( AuthorIterate.PROP_KEY, Property);
			AuthorIterate.propertyIndex.add( PropertyNode, AuthorIterate.PROP_KEY, Property);
		}
	
		Boolean hasRelation = false;
		for ( Relationship relationship : AuthorNode.getRelationships( AuthorIterate.RelTypes.Property, Direction.OUTGOING ) )
			if (relationship.getEndNode().equals(PropertyNode))
				hasRelation = true;
			
		if (!hasRelation) {
			Relationship relationship = AuthorNode.createRelationshipTo(PropertyNode, AuthorIterate.RelTypes.Property);
		}
    }
}