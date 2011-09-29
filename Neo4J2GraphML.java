import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;

import org.neo4j.graphdb.Traverser.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;

import com.mongodb.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import org.neo4j.graphalgo.*;
import org.neo4j.graphalgo.impl.shortestpath.*;
import org.neo4j.graphalgo.impl.centrality.*;
import org.neo4j.graphalgo.impl.util.*;
import org.neo4j.helpers.collection.IteratorUtil;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
import java.lang.Math;
import java.lang.Number;
 
public class Neo4J2GraphML {
 
	private static final String myConnString = "jdbc:mysql://localhost/blogger?user=root&password=";
    private static final String DB_BASE = "../base/neo4j";
	private static Index<Node> userIndex; 
	private static Index<Node> tagIndex; 
	private static Index<Relationship> commentIndex;
    private static GraphDatabaseService graphDb;
	private static PathFinder<WeightedPath> dijkstraPath;
	private static Node StartNode;
	private static Node TagNode;

	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	private static DBCollection authorStats;
	private static Connection mysqlConn;
	private static Statement myStm;
	
	
      private static enum RelTypes implements RelationshipType
    {
		Comments, Tags
    }
 
    public static void main(String[] args) throws Exception {		
		
		/*	
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
		authorStats = mongoDb.getCollection("authorStats");
		collPosts.ensureIndex("postID");
		collPosts.ensureIndex("blogID");				
		collPosts.ensureIndex("authorID");
		*/

		graphDb = new EmbeddedReadOnlyGraphDatabase( DB_BASE );
		userIndex = graphDb.index().forNodes( "authors" );
		tagIndex = graphDb.index().forNodes( "tags" );
		commentIndex = graphDb.index().forRelationships( "comments" );
		
		dijkstraPath = GraphAlgoFactory.dijkstra(
				Traversal.expanderForTypes( RelTypes.Comments, Direction.BOTH ), 
				CommonEvaluators.doubleCostEvaluator("Weight" , 1d) );	
		
		registerShutdownHook();
		
		//computeReciprocity();
		//generateGraphML();
		//authorDegree();
		//tagDegree();
		//authorityGraph();
		//authoritySifGraph();
		//tagCommented();
		tagCommunity("PSDB");
		
        shutdown();
    }	
	
	private static void UpdateRetrieve() throws Exception {
		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = mpQuery.start("value.posts").greaterThanEquals(7).get();
		
		long blogsPop = authorStats.getCount(mpDoc);
		System.out.println(blogsPop);

		for ( DBObject doc : authorStats.find(mpDoc)) {
			System.out.print("+");
			myStm.executeUpdate("UPDATE author SET retrieve = 1 WHERE profileID = '" + doc.get("_id") + "' LIMIT 1");
		}
	}
	
	private static void PopLevel() throws Exception {

		String mapBlogs =	"function(){ " +
							"	emit( this.authorID , { count : 1 , comments : this.comments.length} ); "+
							"};";	
							
        String reduceAvg = "function( key , values ){ "+
							"	var totPosts = 0; var totCom = 0; " +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		totPosts += values[i].count; "+
							"		totCom += values[i].comments; "+
							"   } " +
							"	return { posts: totPosts, comments: totCom, avg: totCom/totPosts } ; "+
							"};";
									
        if (authorStats.getCount() == 0) {
			MapReduceOutput output = collPosts.mapReduce(mapBlogs, reduceAvg, "authorStats", MapReduceCommand.OutputType.REPLACE, null);
			authorStats = output.getOutputCollection();
		}
		
		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = mpQuery.start("value.avg").greaterThanEquals(1).get();
		
		DBObject dbSort = new BasicDBObject();
		dbSort.put("value.avg", -1);
		
		long blogsPop = authorStats.getCount(mpDoc);
		System.out.println(blogsPop);
		
		TraversalDescription travDesc = Traversal.description()
			.breadthFirst()
			.relationships( RelTypes.Comments )
			.uniqueness( Uniqueness.NODE_GLOBAL )
			.evaluator(Evaluators.atDepth(1));	
			
		
		for ( DBObject doc : authorStats.find(mpDoc).sort(dbSort)) {
			
			System.out.println(doc.toString());
			
			Node popNode = userIndex.query("profileId" , doc.get("_id")).getSingle();
			if (popNode!=null) {
				
				
				
				/*myStm.executeUpdate("UPDATE author SET popLevel = 10 WHERE profileID = '" + popNode.getProperty("profileId") + "' LIMIT 1");
				for (Node friend : travDesc.traverse( popNode ).nodes() ) {
					myStm.executeUpdate("UPDATE author SET popLevel = 9 WHERE profileID = '" + friend.getProperty("profileId") + "' LIMIT 1");
				}*/
				
			} else
				System.out.print("-");
			break;
		}
		
	
	}
	
	private static void tagCommented() throws Exception
	{
	
		String mapComTags = 	"function(){ "+
							"	var comments = this.comments.length;"+
							"	this.tags.forEach( "+
							"		function(tag){ "+
							"			emit( tag.toLowerCase() , comments ); "+
							"		} "+
							"	); "+
							"};";
		
        String reduceTags = "function( key , values ){ "+
							"	var com = 0;" +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		com += values[i]; "+
							"   } " +
							"	return com; "+
							"};";
							
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).get();		
		
		DBCollection tagsCommented = mongoDb.getCollection("tagsCommented");
		if (tagsCommented.getCount() == 0) {
			MapReduceOutput output = collPosts.mapReduce(mapComTags, reduceTags, "tagsCommented", MapReduceCommand.OutputType.REPLACE, docQuery);
			tagsCommented = output.getOutputCollection();
		}
		
		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = mpQuery.start("value").greaterThanEquals(1000).get();
		
		DBObject dbSort = new BasicDBObject();
		dbSort.put("value", -1);
		
		for ( DBObject doc : tagsCommented.find(mpDoc).sort(dbSort)) {
			System.out.println(doc.toString());
			myStm.executeUpdate("INSERT INTO tags SET tag = '" + doc.get("_id").toString().replace("'","") + "'");
			myStm.executeUpdate("UPDATE tags SET degree = " + doc.get("value") + " WHERE tag = '" + doc.get("_id").toString().replace("'","") + "' LIMIT 1");
		}
	
	}
	
	private static void tagDegree() throws Exception
	{
		for ( Node node : tagIndex.query("term" , "*") ) {
			if (node.hasProperty("term")) {
				myStm.executeUpdate("INSERT INTO tags SET tag = '" + node.getProperty("term").toString().replace("'","") + "'");
				int size = IteratorUtil.count(node.getRelationships(RelTypes.Tags, Direction.INCOMING));
				myStm.executeUpdate("UPDATE tags SET degree = " + size + " WHERE tag = '" + node.getProperty("term").toString().replace("'","") + "' LIMIT 1");
			}
		}
	
	}
	
	private static void authorDegree() throws Exception
	{
		for ( Node node : userIndex.query("profileId" , "*") ) {
			try {
				if (node.hasProperty("profileId")) {
					int size = IteratorUtil.count(node.getRelationships(RelTypes.Comments, Direction.OUTGOING));
					myStm.executeUpdate("UPDATE author SET degree = " + size + " WHERE profileID = '" + node.getProperty("profileId") + "' LIMIT 1");
				}
			} catch (Exception e) {
				System.out.println( node.getId() );
			}
		}
	
	}
	
	private static int graphDiameter(final Node beginNode, Collection<Node> colNodes)
	{
		SingleSourceShortestPathBFS singleSourceShortestPath = new
			SingleSourceShortestPathBFS(beginNode, Direction.BOTH, RelTypes.Comments);
		IntegerComparator intComp = new IntegerComparator();
		Integer zeroValue = 0;
		
		Set<Node> nodeSet = new HashSet<Node>();
		nodeSet.addAll(colNodes);
		
		NetworkDiameter<Integer> networkDiameter = new 
			NetworkDiameter<Integer>( singleSourceShortestPath, zeroValue, nodeSet, intComp );
				
		return networkDiameter.getCentrality(null);
	}
	
	private static void tagCommunity(final String Term) throws Exception
	{
		Node tagNode = tagIndex.get("term", Term).getSingle();

		Traverser tagCommunity = tagNode.traverse( Order.BREADTH_FIRST ,
			StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE,
			RelTypes.Tags, Direction.INCOMING );

		
		Collection<Node> tagNodes = tagCommunity.getAllNodes();

		Writer output = null;
		File file = new File("tagCommunity.graphml");
		output = new BufferedWriter(new FileWriter(file));  
		
		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<key attr.name=\"weight\" attr.type=\"double\" for=\"edge\" id=\"weight\"/>\n");
		//output.write("<key attr.name=\"indegree\" attr.type=\"int\" for=\"node\" id=\"indegree\"/>\n");
		output.write("<graph edgedefault=\"directed\">\n");

		int contNodes = 0;
		double weight = 1d;
		Map<String, MutableInteger> m_wordFrequency;
		m_wordFrequency = new HashMap<String, MutableInteger>();
		String word = "";
		for ( Node userTag : tagNodes )
		{
			
			for (Relationship rel : userTag.getRelationships(RelTypes.Tags, Direction.OUTGOING)) {
                word = rel.getEndNode().getProperty("term").toString().trim();

                MutableInteger value = m_wordFrequency.get(word);
                if (value == null) {    // Create new entry with count of 1.
                    m_wordFrequency.put(word, new MutableInteger(1));
                } else {                // Increment existing count by 1.
                    value.inc();
                }
                				
			}
			
			for (Relationship rel : userTag.getRelationships(RelTypes.Comments, Direction.OUTGOING)) {
				//if(tagNodes.contains(rel.getEndNode())) {
					if (rel.hasProperty("Weight")) {
						weight = Double.parseDouble(rel.getProperty("Weight").toString());
					}					
					output.write("<edge source=\"" + rel.getStartNode().getId() + "\" target=\"" + rel.getEndNode().getId() + "\">\n");
					output.write("<data key=\"weight\">" + weight + "</data>\n");
					output.write("</edge>\n");
					weight = 1d;
				//}
			}
		}
		

        ArrayList<Map.Entry<String, MutableInteger>> entries;
        entries = new ArrayList<Map.Entry<String, MutableInteger>>(m_wordFrequency.entrySet());
        Collections.sort(entries, new CompareByFrequency());
        
        //... Add word and frequency to parallel output ArrayLists.
        for (Map.Entry<String, MutableInteger> ent : entries) {
        	if (ent.getValue().intValue() > 100)
        	System.out.println( ent.getKey() + ":" + ent.getValue().intValue() );
        }
        
		
		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
		System.out.println( "Counter: " + tagNodes.size() );	
		System.out.println( "Arquivo tagCommunity.graphml gerado!" );		

	}
	
	private static void doubleWeight() {
		Transaction tx = graphDb.beginTx();
		try {
			for ( Node node : userIndex.query("profileId" , "*") ) {
				for (Relationship rel : node.getRelationships()) {
					double Weight = Double.parseDouble(rel.getProperty("Weight").toString()); 
					rel.setProperty("Weight", Weight);
				}
			}
			tx.success();
		} finally {
			tx.finish();
		}
		System.out.println( "Fim da conversão Double." );
	}
	
	private static StopEvaluator stopDepth = new StopEvaluator()
	{
		public boolean isStopNode( TraversalPosition position )
		{
			if ( position.depth() > 2 ) return true;
			return false;
		}
	};
	
	private static boolean hasTagRelation(final Node node, final Node tagNode) {
		for (Relationship rel : node.getRelationships(RelTypes.Tags, Direction.OUTGOING)) {
			if (rel.getEndNode().equals(tagNode)) return true;
		}
		return false;
	}
	
	private static ReturnableEvaluator returnCalc = new ReturnableEvaluator()
	{
		public boolean isReturnableNode( TraversalPosition position )
		{
			if ( position.notStartNode() ) {
				return hasTagRelation(position.currentNode(), TagNode);
				/*
				WeightedPath path = dijkstraPath.findSinglePath(StartNode, position.currentNode());
				int weight = ((Double)path.weight() ).intValue() ;
				return ( weight - path.length() > 0 );
				*/
			}
			return true;
		}
	};
	
	private static void nodeCommunity(final Node node) throws Exception
	{
		StartNode = node;
		Traverser community = StartNode.traverse( Order.BREADTH_FIRST ,
			stopDepth, ReturnableEvaluator.ALL,
			RelTypes.Comments, Direction.BOTH );
		
		Collection<Node> colNodes = community.getAllNodes();
		//int diam = graphDiameter(StartNode, colNodes);

		community = StartNode.traverse( Order.BREADTH_FIRST ,
			stopDepth, returnCalc,
			RelTypes.Comments, Direction.BOTH );

			
		int nodeTagged = community.getAllNodes().size();

		System.out.println(node.toString());
		System.out.println("\t Community Size: " + colNodes.size());
		//System.out.println("\t Community Diameter: " + diam);
		System.out.println("\t Community Tagged Size: " + nodeTagged);


		
	}
	
	private static void computeReciprocity () throws Exception
	{
		int contRel = 0;
		int contRec = 0;
		for ( Node node : userIndex.query("profileId" , "*") ) {
			if (node.hasRelationship(RelTypes.Comments, Direction.INCOMING) && node.hasRelationship(RelTypes.Comments, Direction.OUTGOING)) {
				for (Relationship eIn : node.getRelationships(RelTypes.Comments, Direction.INCOMING)) {
					contRel++;
					for (Relationship eOut : node.getRelationships(RelTypes.Comments, Direction.OUTGOING)) {
						if (eIn.getStartNode().equals(eOut.getEndNode())) {
							contRec++;
						}
					}
				}
			}
		}
		System.out.println( "Fim da reciprocidade: " + contRel + "/" + contRec);
	}
	
	private static void generateGraphML() throws Exception
	{
		Writer output = null;
		File file = new File("neo4j.graphml");
		output = new BufferedWriter(new FileWriter(file));  
		
		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<key attr.name=\"weight\" attr.type=\"double\" for=\"edge\" id=\"weight\"/>\n");
		output.write("<key attr.name=\"indegree\" attr.type=\"int\" for=\"node\" id=\"indegree\"/>\n");
		output.write("<graph edgedefault=\"directed\">\n");
		
		int j = 0;
		int size = 0;
		double weight = 0;
		for ( Relationship rel : commentIndex.query("link" , "*") ) {

			weight = 1;
			try {
				if (rel.hasProperty("Weight")) {
					weight = Double.parseDouble(rel.getProperty("Weight").toString());
				}
			} catch (Exception e) {	}
			if (weight > 2d) {
				output.write("<edge source=\"" + rel.getStartNode().getId() + "\" target=\"" + rel.getEndNode().getId() + "\">\n");
				output.write("<data key=\"weight\">" + weight + "</data>\n");
				output.write("</edge>\n");
			}

			//if (j++ > 1000) break;
		}
		
		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
		System.out.println( "Arquivo neo4j.graphml gerado!" );
	}
	
	private static void authorityGraph() throws Exception
	{
		Writer output = null;
		File file = new File("authority.graphml");
		output = new BufferedWriter(new FileWriter(file));  
		ResultSet rs = null;
		
		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = mpQuery.start("value.comments").greaterThanEquals(20).get();
		
		DBObject dbSort = new BasicDBObject();
		dbSort.put("value.comments", -1);
		
		TraversalDescription travDesc = Traversal.description()
			.breadthFirst()
			.relationships( RelTypes.Comments )
			.uniqueness( Uniqueness.NODE_GLOBAL )
			.evaluator(Evaluators.toDepth(2));	

		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<key attr.name=\"weight\" attr.type=\"double\" for=\"edge\" id=\"weight\"/>\n");
		output.write("<key attr.name=\"indegree\" attr.type=\"int\" for=\"node\" id=\"indegree\"/>\n");
		output.write("<key attr.name=\"local\" attr.type=\"string\" for=\"node\" id=\"local\"/>\n");
		output.write("<key attr.name=\"occup\" attr.type=\"int\" for=\"node\" id=\"occup\"/>\n");
		output.write("<graph edgedefault=\"directed\">\n");			
		
		int j = 0;
		String local = "";
		String occup = "";
		String sql = "";
		for ( DBObject doc : authorStats.find(mpDoc).sort(dbSort)) {
			System.out.println(doc.toString());
			Node node = userIndex.query("profileId" , doc.get("_id")).getSingle();
			int size =0;
			
			for (Node friend : travDesc.traverse( node ).nodes()  ) {
				try {
					local = "";
					/*
					sql = "SELECT Local, (select id from occup where nome = atividade) as occupID FROM author WHERE profileId = '" + friend.getProperty("profileId") + "' LIMIT 1";
					myStm.executeQuery(sql);
					rs = myStm.getResultSet();
					if (rs.first()) {
						if (rs.getString("Local")!=null && rs.getString("Local").equals("BR")) {
							local = "BR";
						}
						if (rs.getString("occupID")!=null) {
							occup = rs.getString("occupID");
						}
					}*/
					size = IteratorUtil.count(friend.getRelationships(RelTypes.Comments, Direction.INCOMING));
					output.write("<node id=\"" + friend.getId() + "\" >\n");
					output.write("<data key=\"indegree\">" + size + "</data>\n");
					//output.write("<data key=\"local\">" + local + "</data>\n");
					//output.write("<data key=\"occup\">" + occup + "</data>\n");
					output.write("</node>\n");	
				} catch (Exception e) {}
			}

			for (Relationship rel : travDesc.traverse( node ).relationships()  ) {
				output.write("<edge source=\"" + rel.getStartNode().getId() + "\" target=\"" + rel.getEndNode().getId() + "\">\n");
				try {
					if (rel.hasProperty("Weight"))
						output.write("<data key=\"weight\">" + rel.getProperty("Weight") + "</data>\n");
					else
						output.write("<data key=\"weight\">1.0</data>\n");
				} catch (Exception e) {
					output.write("<data key=\"weight\">1.0</data>\n");
				}
				output.write("</edge>\n");
			}

		}
		
		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
		System.out.println( "Arquivo authority.graphml gerado!" );
	}
	
	
	private static void authoritySifGraph() throws Exception
	{
		Writer output = null;
		File file = new File("authority.sif");
		output = new BufferedWriter(new FileWriter(file));  
		ResultSet rs = null;
		
		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = mpQuery.start("value.comments").greaterThanEquals(20).get();
		
		DBObject dbSort = new BasicDBObject();
		dbSort.put("value.comments", -1);
		
		TraversalDescription travDesc = Traversal.description()
			.breadthFirst()
			.relationships( RelTypes.Comments )
			.uniqueness( Uniqueness.NODE_GLOBAL )
			.evaluator(Evaluators.toDepth(2));			
		
		int j = 0;
		String local = "";
		String occup = "";
		String sql = "";
		for ( DBObject doc : authorStats.find(mpDoc).sort(dbSort)) {
			System.out.println(doc.toString());
			Node node = userIndex.query("profileId" , doc.get("_id")).getSingle();
			int size =0;

			for (Relationship rel : travDesc.traverse( node ).relationships()  ) {
				output.write(rel.getStartNode().getId() + " comment " + rel.getEndNode().getId() + "\n");
			}

	
		}
		
		output.close();
		
		System.out.println( "Arquivo authority.sif gerado!" );
	}


    private static void shutdown()
    {
		System.out.println("shutdown");
        graphDb.shutdown();
		/*mongoConn.close();
		try {
			myStm.close();
		} catch (Exception ex) {}*/
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
 
class MutableInteger {
    private int m_value;
    
    /** Constructor */
    public MutableInteger(int value) {
        m_value = value;
    }
    
    /** Return int value. */
    public int intValue() {
        return m_value;
    }
    
    /** Increment value */
    public void inc() {
        m_value++;
    }
}

class CompareByFrequency implements Comparator<Map.Entry<String, MutableInteger>> {
    public int compare(Map.Entry<String, MutableInteger> obj1
                     , Map.Entry<String, MutableInteger> obj2) {
        int c1 = obj1.getValue().intValue();
        int c2 = obj2.getValue().intValue();
        if (c1 < c2) {
            return 1;
            
        } else if (c1 > c2) {
            return -1;
            
        } else { // If counts are equal, compare keys alphabetically.
            return obj1.getKey().compareTo(obj2.getKey());
        }
    }
}
