<?php
require('neo4j.blog.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$TagIndex = new IndexService( $graphDb , 'node', 'tags');
$TagRelIndex = new IndexService( $graphDb , 'relationship', 'tagRel');

$allAuthors = $AuthorsIndex->getAllNodes();
echo "Autores: ".count($allAuthors)."<br>\n";
echo "Tags: ".count($TagIndex->getAllNodes())."<br>\n";
echo "RelTags: ".count($TagRelIndex->getAllNodes())."<br>\n";

/*$allComments = array();
foreach ($allAuthors as $id => $author) {
	$relComments = $author->getRelationships(Relationship::DIRECTION_BOTH, "Comments");
		foreach ($relComments as $rel)	
			$allComments[$rel->getId()] = 1;
	if (($id+1) % 100 == 0) {
		echo "($id) Comments: ".count($allComments)."<br>\n";	
	}
}

echo "Comments: ".count($allComments)."<br>\n";	*/
