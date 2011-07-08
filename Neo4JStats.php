<?php
/*
Installing Gremlin Plugin
http://romikoderbynew.wordpress.com/2011/06/11/neo4j-and-gremlin-plugin-install-guide/
*/
require('neo4j.blog.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');

$numAuthors = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*').count();");
$numUnsetAuthors = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV._(){it.blogs==null}._(){it.blogsSet==null}.count();");
$numBrAuthors = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','BR')._().bothE.count();");
//$numBlogs = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.blogs}._().blogs.toList().toString().size() - g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.blogs}._().blogs.toList().toString().replace(\",\",\"\").size();");
$numBlogsBR = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV._(){it.blogs}._().blogs.toList().toString().size() - g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV._(){it.blogs}._().blogs.toList().toString().replace(\",\",\"\").size();");
//$numComments = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._().bothE{it.label=='Comments'}.count();");
//$numTags = $graphDb->gremlinExec("g.getIndex('tags',Vertex.class).get('term',Neo4jTokens.QUERY_HEADER+'*').count();");
//$numAuthorsUnk = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.Local==null}.count();");

$arAct = array('AGRICULTURE','STUDENT','ENVIRONMENT','ARCHITECTURE','ARTS','LAW','AUTOMOTIVE','BANKING','INVESTMENT_BANKING','BIOTECH','SCIENCE','COMMUNICATIONS_OR_MEDIA','CONSTRUCTION','CONSULTING','ACCOUNTING','LAW_ENFORCEMENT_OR_SECURITY','ENGINEERING','EDUCATION','SPORTS_OR_RECREATION','GOVERNMENT','REAL_ESTATE','INTERNET','MANUFACTURING','MARITIME','MARKETING','MILITARY','FASHION','MUSEUMS_OR_LIBRARIES','CHEMICALS','PUBLISHING','ADVERTISING','HUMAN_RESOURCES','RELIGION','NON_PROFIT','BUSINESS_SERVICES','TECHNOLOGY','TELECOMMUNICATIONS','TRANSPORTATION','TOURISM');
foreach ($arAct as $act) {
	$count = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','$act')._().bothE.count();");
	$countAll += $count;
	$arRes[$count] = $act;
}
krsort($arRes);

//echo "Autores: $numAuthors\n";
echo "Autores BR: $numBrAuthors\n";
echo "Autores unset: $numUnsetAuthors\n";
//echo "Autores sem Local: $numAuthorsUnk\n";
//echo "Blogs: $numBlogs\n";
echo "Blogs BR: $numBlogsBR\n";
//echo "Comentarios: ($numComments/2)\n";
//echo "Tags: $numTags\n";
echo  "Autores com Atividade: $countAll\n";

print_r($arRes);
