<?php
require_once('neo4j.blog.php');
require_once('blogger.php');
require('index.php');

$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');

$ctAuthors = $graphDb->gremlinExec("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.blogSet!=1}._(){it.blogs}.count();");
echo "Iteration over ".$ctAuthors." nodes:\n";

	for ($iA=1;$iA<$ctAuthors;$iA++) {
		$author = $graphDb->gremlinNode("g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.blogSet!=1}._(){it.blogs}[0];");
		echo $iA."/".$ctAuthors."(".$author->getId().")\n";

		$arBlogs = array();
		foreach ($author->blogs as $blogID) {
			if ($xmlstr = file_get_contents("http://www.blogger.com/rsd.g?blogID=$blogID")) {
				$xml = new SimpleXMLElement($xmlstr);
				preg_match_all("/\/([^\/]+)\//", $xml->service->homePageLink[0], $blog);
				$arBlogs[] = $blog[1][0];
			} else {
				$erroHandle = error_get_last();
				if (strpos($erroHandle['message'],'404 Not Found') || strpos($erroHandle['message'],'500 Internal Server')) {
					$author->info = 1;
					$author->save();
				}
			}
		}
		if (count($arBlogs)==count($author->blogs)) {
			$author->blogs = $arBlogs;
			$author->blogSet = 1;
			$author->save();
		}
	
	}
