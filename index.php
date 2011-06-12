<?php
/*
$ index -t Node --create authors 
$ index -t Node --create tags 
$ index -t Node --create property
$ index -t Relationship --create tagRel 

(php command link) php.exe -f script.php
*/
require_once('neo4j.blog.php');
require_once('blogger.php');

function getAuthorBlogs($profileID) {

$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$TagIndex = new IndexService( $graphDb , 'node', 'tags');
$TagRelIndex = new IndexService( $graphDb , 'relationship', 'tagRel');
$bloggerUrl = "http://www.blogger.com/feeds/";

if ($profileID) {
	//Retrieving Profile
	if ($xmlstr = file_get_contents($bloggerUrl."$profileID/blogs")) {
		$xml = new SimpleXMLElement($xmlstr);

		$blogID = array();
		foreach ($xml->entry as $entry)
			$blogID[] = Blogger::findIdByUri($entry->link[3]->attributes()->href);
		
		$authorNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
		$authorNode->id = $profileID;
		$authorNode->name = Blogger::normalize($xml->author->name);
		if ($blogID) $authorNode->blogs = $blogID;
		$authorNode->save();
		
		if (false && $xml->entry->category && !$authorNode->tags) //Tags have hug overhead
			foreach ($xml->entry->category as $cat) {
				$tagNode = new IndexNode($graphDb, $TagIndex, 'term');
				$tagNode->term = Blogger::normalize($cat->attributes()->term);
				$tagNode->save();
				$authorNode->createIndexRelationshipTo($TagRelIndex, $tagNode, 'Tag');
			}
		
		//Retrieving BlogID Comments
		foreach ($blogID as $id) {
			if (@$xmlstr = file_get_contents($bloggerUrl."$id/comments/default")) {
				$commentAr = array();
				$xml = new SimpleXMLElement($xmlstr);
				foreach ($xml->entry as $entry) {
					if (Blogger::findIdByUri($entry->author->uri)) {
						$commentNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
						$commentNode->id = Blogger::findIdByUri($entry->author->uri);
						$commentNode->name = Blogger::normalize($entry->author->name);
						$commentNode->save();
						$commentNode->createRelationshipTo($authorNode, 'Comments');
						$commentAr[] = $commentNode->id . "(". $commentNode->name. ")" . "(". $commentNode->getId(). ")";
					}
				}
			}
		}
		$authorNode->blimp = 1;
		$authorNode->save();
		echo "ok";
	} else {
			$erroHandle = error_get_last();
			if (strpos($erroHandle['message'],'404 Not Found')) {
				$authorNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
				$authorNode->id = $profileID;
				$authorNode->blimp = 1;
				$authorNode->save();
			}
		}
}

}

getAuthorBlogs($_GET['profileID']);