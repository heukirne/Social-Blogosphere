import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;

import com.mongodb.*;

import com.google.gdata.client.Query;
import com.google.gdata.client.blogger.BloggerService;
import com.google.gdata.data.*;
import com.google.gdata.util.ServiceException;

import net.sf.json.JSONObject;
import net.sf.json.JSONArray;

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
 
/**
 * Example class that constructs a simple graph with message attributes and then prints them.
 */
public class AuthorIterate {
 
	private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    private static final String DB_BLOG = "D:/xampplite/neo4j/data/graph.db";
	private static final String myConnString = "jdbc:mysql://localhost/blogs?user=root&password=";
	private static final String DB_BASE = "base/neo4j";
	private static final String AUTHOR_KEY = "profileId";
	private static final String COMMENT_KEY = "link";
	private static final String TAG_KEY = "term";
	private static final String POST_KEY = "post";
	private static final String BLOG_KEY = "blog";
	private static final String PROP_KEY = "info";
    private static GraphDatabaseService graphDb;
	private static GraphDatabaseService readonlyDb;
    private static Index<Node> userIndex; 
	private static Index<Node> tagIndex; 
	private static Index<Node> postIndex;
	private static Index<Node> blogIndex;
	private static Index<Node> propertyIndex;
	private static Index<Relationship> commentIndex; 
	private static Node dummyNode;
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	private static Connection mysqlConn;
	private static Statement myStm;
 
     private static enum RelTypes implements RelationshipType
    {
        Comments,
        Tags,
		Property
    }

	 public static int relationSize(Iterable<Relationship> iterable) {
		int cont = 0;
		for (Relationship relation : iterable) cont++;
		return cont;
	  }
	
    public static void main(String[] args) throws Exception {		
				
		mysqlConn = DriverManager.getConnection(myConnString);
		myStm = mysqlConn.createStatement();
		myStm.executeQuery("set wait_timeout = 7200");
		
		
		graphDb = new EmbeddedGraphDatabase( DB_BASE );
        userIndex = graphDb.index().forNodes( "authors" );
		tagIndex = graphDb.index().forNodes( "tags" );
		postIndex = graphDb.index().forNodes( "posts" );
		blogIndex = graphDb.index().forNodes( "blogs" );
		commentIndex = graphDb.index().forRelationships( "comments" );
		registerShutdownHook();

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		collPosts = mongoDb.getCollection("posts");
		collPosts.ensureIndex("linkID");		
		
		getBlogs();
		
		Transaction tx = graphDb.beginTx();
		try {	
			System.out.println("Numbers of Users:" + userIndex.query( AUTHOR_KEY , "*" ).size() );
			System.out.println("Numbers of Blogs:" + blogIndex.query( BLOG_KEY , "*" ).size());
			System.out.println("Numbers of Tags:" + tagIndex.query( TAG_KEY , "*" ).size());
			System.out.println("Numbers of Posts:" + postIndex.query( POST_KEY , "*" ).size());
			System.out.println("Numbers of Comments:" + commentIndex.query( COMMENT_KEY , "*" ).size());
			tx.success();
		} finally {
			tx.finish();
		}
		
        System.out.println( "Shutting down database ..." );
        shutdown();
    }

	public static void getBlogs() throws Exception {

		ResultSet rs = null;
		String[] blogs = null;

		for(int j = 3000 ; j <= 10000; j++)
		{
			
			blogs = null;
			myStm.executeQuery("SELECT blogs FROM author WHERE Local = 'BR' and length(Blogs) > 2 ORDER BY profileID LIMIT "+ j +",1");
			rs = myStm.getResultSet();
			if (rs!=null) {
				rs.next();
				blogs = Pattern.compile(",").split(rs.getString("blogs"));
			}
		
			if (blogs==null) continue;
			for (String blog : blogs)
			{
				Transaction tx = graphDb.beginTx();
				try {
					String blogID = blog.trim().replace("http:","").replace("/","");
					//System.out.println(blogID);
					if (blogIndex.get( BLOG_KEY, blogID).size()==0) {
						//System.out.println(blogID);
						if (getPosts(blogID)) {
							//Blog Index only to store visited blogs
							Node blogNode = graphDb.createNode();
							blogNode.setProperty( BLOG_KEY, blogID);
							blogIndex.add(blogNode, BLOG_KEY, blogID);
						} 
					}
					tx.success();
				} finally {
					tx.finish();
				}
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
			}
			
		}
		
	}
	
	public static Boolean getPosts(final String blogUri) throws ServiceException, IOException {
			
			System.out.print("Retriving:" + blogUri);			
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			Transaction tx = graphDb.beginTx();
			
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
					String postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
					if (entry.getAuthors().get(0).getUri()!=null && postIndex.get( POST_KEY, postID).size()==0) {
					
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
						Node postNode = graphDb.createNode();
						postNode.setProperty( POST_KEY, postID);
						postIndex.add(postNode, POST_KEY, postID);
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
				return false;
			} catch (ServiceException e) {
				System.out.println("Service Exception!");
				return true;
			} finally {
				tx.finish();
			}
	}
	
	public static ArrayList<Node> getComments(final String postUri) throws ServiceException, IOException {
			//System.out.print(" " + postUri + ",");
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
	
	private static void setMongoPost(Entry entry) {
		
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
	
	private static void setMongoComment(String postUri, Entry entry) {

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
	
    private static Node createAuthorNode( final String profileID ) throws Exception
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
	
	private static void updateDegree(Node node) throws Exception
	{
		if (node.hasProperty(AUTHOR_KEY)) {
			myStm.executeUpdate("INSERT INTO author SET profileID = '" + node.getProperty(AUTHOR_KEY) + "'");
			int size = relationSize(node.getRelationships(RelTypes.Comments, Direction.BOTH));
			myStm.executeUpdate("UPDATE author SET degree = " + size + " WHERE profileID = '" + node.getProperty(AUTHOR_KEY) + "' LIMIT 1");
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
					relationship.setProperty("Weight", Weight+1);
					commentIndex.add(relationship , COMMENT_KEY, Uri);
				}
			if (!hasRelation) {
				Relationship relationship = CommentNode.createRelationshipTo(AuthorNode, RelTypes.Comments);
				relationship.setProperty("Weight", 1);
				commentIndex.add(relationship , COMMENT_KEY, Uri);
			}
		}
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
	
    private static String GremlinExecute(String script) throws Exception
    {
		String GremlinURI = SERVER_ROOT_URI + "ext/GremlinPlugin/graphdb/execute_script";

		// Send data
		URLConnection urlConnection = new URL(GremlinURI).openConnection();
		
		urlConnection.setDoOutput(true);
		urlConnection.setDoInput(true);
		//urlConnection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
		urlConnection.setRequestProperty("Accept", "application/json");
		
		OutputStreamWriter wr = new OutputStreamWriter(urlConnection.getOutputStream());
		wr.write(URLEncoder.encode("script", "UTF-8") + "=" + URLEncoder.encode(script, "UTF-8"));
		wr.flush();

		// Get the response
		BufferedReader rd = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
		String line;
		String json = "";
		while ((line = rd.readLine()) != null) {
			json += line;
		}
		wr.close();
		rd.close();
		
		return json;
	}
	
    private static JSONObject GremlinNode(String script) throws Exception
    {
		String json = GremlinExecute(script);
	
		json = json.replace("[ {","{").replace("} ]","}");
		return JSONObject.fromObject(json);
	}
	
	private static ResultSet querySQL(final String sql) throws Exception
	{
		Boolean bExec = true;
		while (bExec) {
			try {	
				myStm.executeQuery(sql);
				bExec = false;
			} catch (SQLException ex){ 
				bExec = false;
			} catch (Exception ex){ 
				System.out.println("<Reconnect MySQL>"); 
				mysqlConn = DriverManager.getConnection(myConnString);
				myStm = mysqlConn.createStatement();
			}
		}
		return myStm.getResultSet();
	}
	
	private static void updateSQL(final String sql) throws Exception
	{
		Boolean bExec = true;
		while (bExec) {
			try {	
				myStm.executeUpdate(sql);
				bExec = false;
			} catch (SQLException ex){ 
				bExec = false;
			} catch (Exception ex){ 
				System.out.println("<Reconnect MySQL>"); 
				mysqlConn = DriverManager.getConnection(myConnString);
				myStm = mysqlConn.createStatement();
			}
		}
	}
	
    private static void shutdown()
    {
        graphDb.shutdown();
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
