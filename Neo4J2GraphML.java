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
 
public class Neo4J2GraphML {
 
	private static final String myConnString = "jdbc:mysql://localhost/gemeos110?user=gemeos110&password=dias09ufrgs";
    private static final String DB_BASE = "base/neo4j";
	private static Index<Node> userIndex; 
	private static Index<Node> tagIndex; 
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
	
 	 public static int relationSize(Iterable<Relationship> iterable) {
		int cont = 0;
		for (Relationship relation : iterable) cont++;
		return cont;
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
		
		graphDb = new EmbeddedReadOnlyGraphDatabase( DB_BASE );
		userIndex = graphDb.index().forNodes( "authors" );
		tagIndex = graphDb.index().forNodes( "tags" );
		
		dijkstraPath = GraphAlgoFactory.dijkstra(
				Traversal.expanderForTypes( RelTypes.Comments, Direction.BOTH ), 
				CommonEvaluators.doubleCostEvaluator("Weight" , 1d) );	
		
		registerShutdownHook();
		
		//computeReciprocity();
		//generateGraphML();
		//authorDegree();
		//tagDegree();
		
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
		
		long blogsPop = authorStats.getCount(mpDoc);
		System.out.println(blogsPop);
		
		TraversalDescription travDesc = Traversal.description()
			.breadthFirst()
			.relationships( RelTypes.Comments )
			.uniqueness( Uniqueness.NODE_GLOBAL )
			.evaluator(Evaluators.atDepth(1));	
			
		
		for ( DBObject doc : authorStats.find(mpDoc)) {
			Node popNode = userIndex.query("profileId" , doc.get("_id")).getSingle();
			if (popNode!=null) {
				System.out.print("+");
				
				myStm.executeUpdate("UPDATE author SET popLevel = 10 WHERE profileID = '" + popNode.getProperty("profileId") + "' LIMIT 1");

				for (Node friend : travDesc.traverse( popNode ).nodes() ) {
					myStm.executeUpdate("UPDATE author SET popLevel = 9 WHERE profileID = '" + friend.getProperty("profileId") + "' LIMIT 1");
				}
			} else
				System.out.print("-");
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
			if (node.hasProperty("profileId")) {
				int size = IteratorUtil.count(node.getRelationships(RelTypes.Comments, Direction.BOTH));
				myStm.executeUpdate("UPDATE author SET degree = " + size + " WHERE profileID = '" + node.getProperty("profileId") + "' LIMIT 1");
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
		TagNode = tagIndex.get("term", Term).getSingle();
		for (Relationship relTag : TagNode.getRelationships(RelTypes.Tags, Direction.INCOMING)) {
			nodeCommunity(relTag.getStartNode());
			break;
		}	
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
		}
		
		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
		System.out.println( "Arquivo neo4j.graphml gerado!" );
	}
	
    private static void shutdown()
    {
		System.out.println("shutdown");
        graphDb.shutdown();
		mongoConn.close();
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
 