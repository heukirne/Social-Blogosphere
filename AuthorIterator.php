<?php
require('neo4j.blog.php');
require('blogger.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$getAtuhor = "http://localhost/Social-Blogosphere/index.php?profileID=";
	$ct = 1;
	$allAuthors = $AuthorsIndex->getAllNodes();
	$ctAuthors = count($allAuthors);
	echo "Iteration over ".$ctAuthors." nodes:\n";
	foreach ($allAuthors as $id => $author) {
		if (!$author->blimp) {
			echo $id."/".$ctAuthors."-".$author->id."(".$author->getId().")";
			echo file_get_contents($getAtuhor . "$author->id");
			echo " links(".count($author->getRelationships(Relationship::DIRECTION_BOTH,'Comments')).")";
			echo "\n";
		}
		
	}
	$allAuthors = $AuthorsIndex->getAllNodes();
	echo "Add news Authors:".(count($allAuthors)-$ctAuthors)."\n";
