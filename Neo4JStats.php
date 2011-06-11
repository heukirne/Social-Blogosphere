<?php
require('neo4j.blog.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$TagIndex = new IndexService( $graphDb , 'node', 'tags');
$TagRelIndex = new IndexService( $graphDb , 'relationship', 'tagRel');


echo "Autores: ".count($AuthorsIndex->getAllNodes())."<br>\n";
echo "Tags: ".count($TagIndex->getAllNodes())."<br>\n";
echo "RelTags".count($TagRelIndex->getAllNodes())."<br>\n";


$allAuthors = $AuthorsIndex->getAllNodes();
$allComments = array();
foreach ($allAuthors as $author)
	$relComments = $author->getRelationships(Relationship::DIRECTION_BOTH, "Comments");
		foreach ($relComments as $rel)	
			$allComments[$rel->getId()] = 1;

echo "Comments:".count($allComments)."<br>\n";	
