<?php
require_once('neo4j.blog.php');
require_once('blogger.php');
require('index.php');

$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');

	$ct = 1;
	$allAuthors = $AuthorsIndex->getNodesByQuery('id','1*');
	$ctAuthors = count($allAuthors);
	echo "Iteration over ".$ctAuthors." nodes:\n";
	foreach ($allAuthors as $id => $author) {
		if (!$author->blimp) {
			echo $id."/".$ctAuthors."-".$author->id."(".$author->getId().")";
			getAuthorBlogs($author->id);
			echo " links(".count($author->getRelationships(Relationship::DIRECTION_BOTH,'Comments')).")";
			echo "\n";
		}
		
	}
	$allAuthors = $AuthorsIndex->getAllNodes();
	echo "Add news Authors:".(count($allAuthors)-$ctAuthors)."\n";
