<?php
/*
Installing Gremlin Plugin
http://romikoderbynew.wordpress.com/2011/06/11/neo4j-and-gremlin-plugin-install-guide/
*/
require('neo4j.blog.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');

$numAuthors = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*').count();");
$numBrAuthors = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','BR')._().bothE.count();");
$numBlogs = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._().blogs.count();");
$numComments = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._().bothE{it.label=='Comments'}.count();");
$numTags = $graphDb->gremlinExec("g.getIndex('tags',Vertex.class).get('term',Neo4jTokens.QUERY_HEADER+'*').count();");

echo "Autores: ".$numAuthors."\n";
echo "Autores BR: ".$numBrAuthors."\n";
echo "Blogs?: ".$numBlogs."\n";
echo "Comentarios: ".$numComments."\n";
echo "Tags: ".$numTags."\n";