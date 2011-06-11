<?php
// Google don't like flood requests
require('neo4j.blog.php');
require('blogger.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$getAtuhorInfo = "http://www.blogger.com/profile/";

$allAuthors = $AuthorsIndex->getAllNodes();
$ctAuthors = count($allAuthors);
echo "Iteration over ".$ctAuthors." nodes:\n";

foreach ($allAuthors as $id => $author) {

	$html="";
	echo $id."/".$ctAuthors."-".$author->id."(".$author->getId().")";
	if (!$author->info)
	{
		if ($html = file_get_contents($getAtuhorInfo.$author->id)) {
			$html = str_replace('strong','b',$html);
			preg_match_all("/<b>([^<]*)<\/b>(\n)?([^<]*)(.*)/", $html, $listItens); 
			echo "(html)";
			
			$prop = array();
			foreach ($listItens[1] as $i => $name) 
				if (strpos($name,':')) {
					$name = str_replace(':','',$name);
					$name = str_replace(' ','_',$name);
					$name = Blogger::normalize($name);
					$prop[$name] = $listItens[3][$i]?$listItens[3][$i]:$listItens[4][$i];
				}

			if (!empty($prop)) {
				echo "(prop)";
				foreach ($prop as $i => $value) {
					$result = array();
					if (strpos($value,'role')) preg_match("/ind=([^\"]*)/",$value,$result);
					if (strpos($value,'title')) preg_match("/q=([^\"]*)/",$value,$result);
					if (strpos($value,'locality')) preg_match("/loc0=([^\&]*)/",$value,$result);
					$prop[$i] = Blogger::normalize((empty($result))?$value:$result[1]);
				}
				
				$author->setProperties(array_merge($author->getProperties(),$prop));
				$author->info = 1;
				$author->save();
			} 
		}
	} else { echo " jump!"; }
	echo "\n";
	
}
