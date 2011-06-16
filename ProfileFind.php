<?php
// Google don't like flood requests
require('neo4j.blog.php');
require('blogger.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$PropIndex = new IndexService( $graphDb , 'node', 'property');

$local = 'BR';

/*
Set INFO = 1 for Nodes with Property
g.getIndex('authors',Vertex.class).get('id',Neo4jTokens.QUERY_HEADER+'*')._(){it.info!=1}.outE{it.label=="Property"}.outV._(){it.info=1}.count();
*/
$getAtuhorBR = "http://www.blogger.com/profile-find.g?t=l&loc0=$local&start=:i&ct=:ct";
$numBrAuthors = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','$local')._().bothE.count();");

$brNode = new IndexNode($graphDb, $PropIndex, 'info');
$brNode->info = $local;
$brNode->save();

$ct = array(1 => array(0 => ""));
for ($i=0;$i<4491;$i+=10) {

		$urlNow = str_replace(':i',$i,$getAtuhorBR);
		$urlNow = str_replace(':ct',$ct[1][0],$urlNow);
		
		//echo $urlNow."\n";
		if ($html = file_get_contents($urlNow)) {			
			preg_match_all("/<h2><a href=\"http:\/\/www.blogger.com\/profile\/(\d+)/", $html, $listProfile); 
			if (empty($ct[1][0])) preg_match_all("/ct=([^\"]+)/", $html, $ct);

			foreach ($listProfile[1] as $id) {
				$authorNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
				$authorNode->id = $id;
				$authorNode->name = Blogger::normalize($xml->author->name);
				$authorNode->update = 1;
				$authorNode->info = 1;
				$authorNode->save();
				$authorNode->createRelationshipTo($brNode, 'Property');
			}
			
			$newBrAuthors = $graphDb->gremlinExec("g.getIndex('property',Vertex.class).get('info','$local')._().bothE.count();") - $numBrAuthors;
			
			echo "New Authors $local: $newBrAuthors!\n"; sleep(30);
		} else {
			$erroHandle = error_get_last();
		}	

}
