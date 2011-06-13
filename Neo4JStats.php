<?php
/*
Gremlin Syntax

g.getIndex('tags',Vertex.class).get('term','psdb');
g.getIndex('tags',Vertex.class).get('term',Neo4jTokens.QUERY_HEADER+'psd*');
g.getIndex('tags',Vertex.class).get('term',Neo4jTokens.QUERY_HEADER+'psd*')._().bothE{it.label=="Tag"}.count();
g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._().bothE{it.label=="Comments"}.count();
g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*').count();
g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._().blogs.count();
g.getIndex('property',Vertex.class).get('info','BR')._().bothE.count();
*/
require('neo4j.blog.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$TagIndex = new IndexService( $graphDb , 'node', 'tags');
$TagRelIndex = new IndexService( $graphDb , 'relationship', 'tagRel');

$allAuthors = $AuthorsIndex->getAllNodes();

echo "Autores: ".count($allAuthors)."\n";
echo "Tags: ".count($TagIndex->getAllNodes())."\n";
echo "RelTags: ".count($TagRelIndex->getAllNodes())."\n";

/* Big Overhead in Neo4J for counting Comments
$allComments = array();
foreach ($allAuthors as $id => $author) {
	$relComments = $author->getRelationships(Relationship::DIRECTION_BOTH, "Comments");
		foreach ($relComments as $rel)	
			$allComments[$rel->getId()] = 1;
	if (($id+1) % 100 == 0) {
		echo "($id) Comments: ".count($allComments)."\n";	
	}
}

echo "Comments: ".count($allComments)."\n";	
*/

$allBlogs = array();
foreach ($allAuthors as $id => $author) {
	if (is_array($author->blogs))
		foreach ($author->blogs as $blog);
			$allBlogs[$blog] = 1;
}
echo "Blogs: ".count($allBlogs)."\n";

