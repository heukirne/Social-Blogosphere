<?php
require_once('neo4j.blog.php');
require_once('blogger.php');
require('index.php');

$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
/*
only BR: g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV{it.blimp!=1}.count();
everyone: g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.blimp!=1}.count();
*/
$ctAuthors = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV{it.blimp!=1}.count();");
echo "Iteration over ".$ctAuthors." nodes:\n";

	for ($iA=1;$iA<$ctAuthors;$iA++) {
		$author = $graphDb->gremlinNode("g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV{it.blimp!=1}[0];");
		echo $iA."/".$ctAuthors."-".$author->id."(".$author->getId().")";
		getAuthorBlogs($author->id);
		echo " links(".count($author->getRelationships(Relationship::DIRECTION_BOTH,'Comments')).")";
		echo "\n";		
	}
	$allAuthors = $AuthorsIndex->getAllNodes();
	echo "Add news Authors:".(count($allAuthors)-$ctAuthors)."\n";
