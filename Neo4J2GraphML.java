import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.kernel.*;
import org.neo4j.graphdb.Traverser.*;
import org.neo4j.graphalgo.*;
import org.neo4j.graphalgo.impl.shortestpath.*;
import org.neo4j.graphalgo.impl.centrality.*;
import org.neo4j.graphalgo.impl.util.*;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
import java.lang.Math;
 
public class Neo4J2GraphML {
 
    private static final String DB_BASE = "base/neo4j";
	private static Index<Node> userIndex; 
	private static Index<Node> tagIndex; 
    private static GraphDatabaseService graphDb;
	private static PathFinder<WeightedPath> dijkstraPath;
	private static Node StartNode;
	private static Node TagNode;

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
				
		graphDb = new EmbeddedReadOnlyGraphDatabase( DB_BASE );
		userIndex = graphDb.index().forNodes( "authors" );
		tagIndex = graphDb.index().forNodes( "tags" );
		
		dijkstraPath = GraphAlgoFactory.dijkstra(
				Traversal.expanderForTypes( RelTypes.Comments, Direction.BOTH ), 
				CommonEvaluators.doubleCostEvaluator("Weight" , 1d) );	
		
		registerShutdownHook();
		
		tagCommunity("Esporte");
		
        shutdown();
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
		int diam = graphDiameter(StartNode, colNodes);
		
		community = StartNode.traverse( Order.BREADTH_FIRST ,
			stopDepth, returnCalc,
			RelTypes.Comments, Direction.BOTH );
			
		int nodeTagged = community.getAllNodes().size();
		
		System.out.println(node.toString());
		System.out.println("\t Community Size: " + colNodes.size());
		System.out.println("\t Community Diameter: " + diam);
		System.out.println("\t Community Tagged Size: " + nodeTagged);
		
	}
	
	private static void computeReciprocity () throws Exception
	{
		for ( Node node : userIndex.query("profileId" , "*") ) {
			if (node.hasRelationship(RelTypes.Comments, Direction.INCOMING) && node.hasRelationship(RelTypes.Comments, Direction.OUTGOING)) {
				for (Relationship eIn : node.getRelationships(RelTypes.Comments, Direction.INCOMING)) {
					for (Relationship eOut : node.getRelationships(RelTypes.Comments, Direction.OUTGOING)) {
						if (eIn.getStartNode().equals(eOut.getEndNode())) {
							System.out.print("Reciprocity: ");
							System.out.print(eIn.getStartNode().toString() + " <-> ");
							System.out.println(eIn.getEndNode().toString());
						}
					}
				}
				break;
			}			
		}
		System.out.println( "Fim da reciprocidade." );
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
 