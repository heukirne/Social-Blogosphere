<?php
// Google don't like flood requests
// 231 days for 2 million users profile page each 10s
require('neo4j.blog.php');
require('blogger.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$PropIndex = new IndexService( $graphDb , 'node', 'property');
$getAuthorInfo = "http://www.blogger.com/profile/";

$propArray = array('Atividade','Signo_astrologico','Profissao','Sexo');

while (true) {

	$author = $graphDb->gremlinNode("g.getIndex('property',Vertex.class).get('info','BR')._().inE.outV._(){it.blogs==null}._(){it.blogsSet==null}[0];");
	if (empty($author)) break;

	
	$html="";
	echo ($iA++)."-".$author->id."(".$author->getId().")";
	
	if ($html = file_get_contents($getAuthorInfo.$author->id)) {
		$html = str_replace('strong','b',$html);
		preg_match_all("/<b>([^<]*)<\/b>(\n)?([^<]*)(.*)/", $html, $listItens); 
		preg_match_all("/href=\"([^\"]+)\"[^\"]+\"contributor-to/", $html, $blogs); 
	
		if (empty($author->blogs) && !empty($blogs[1]))
				$author->blogs = $blogs[1];
		
		$author->blogsSet = 1;
		
		echo "(html)";
		//print_r($listItens);
		$prop = array();
		foreach ($listItens[1] as $i => $name) 
			if (strpos($name,':')) {
				$name = str_replace(':','',$name);
				$name = str_replace(' ','_',$name);
				$name = Blogger::normalize($name);
				$prop[$name] = trim($listItens[3][$i])?$listItens[3][$i]:$listItens[4][$i];
			}
		//print_r($prop);
		if (!empty($prop)) {
			echo "(prop)";
			foreach ($prop as $i => $value) {
				$result = array();
				if (strpos($value,'role')) preg_match("/ind=([^\"]*)/",$value,$result);
				if (strpos($value,'title')) preg_match("/q=([^\"]*)/",$value,$result);
				if (strpos($value,'loc1')) preg_match("/loc1=(\w*)/",$value,$result);
				if (strpos($value,'loc2')) preg_match("/loc2=(\w*)/",$value,$result);
				if (strpos($value,'loc0')) preg_match("/loc0=(\w{2})/",$value,$result);
				$prop[$i] = Blogger::normalize((empty($result))?$value:$result[1]);
			}
			//print_r($prop);
			//die();
			
			$authorNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
			$authorNode->import($author);
			
			foreach ($prop as $value) {
				if (in_array($prop,$propArray) && !empty($value)) {
					$propNode = new IndexNode($graphDb, $PropIndex, 'info');
					$propNode->info = Blogger::normalize($value);
					$propNode->save();
					$authorNode->createRelationshipTo($propNode, 'Property');
				}
			}
			
			$author->setProperties(array_merge($author->getProperties(),$prop));
			$author->update = 1;
		} 
		
		$author->info = 1;
		$author->save();
		echo "\n";
	} else {
		$erroHandle = error_get_last();
		if (strpos($erroHandle['message'],'404 Not Found') || strpos($erroHandle['message'],'500 Internal Server')) {
			$author->blogsSet = 1;
			$author->save();
		}
	}
	sleep(10);
	
}
