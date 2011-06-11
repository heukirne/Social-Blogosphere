<?php
/*
$ index -t Node --create authors 
$ index -t Node --create tags 
$ index -t Relationship --create tagRel 
*/
require('neo4j.blog.php');
require('blogger.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'node', 'authors');
$TagIndex = new IndexService( $graphDb , 'node', 'tags');
$TagRelIndex = new IndexService( $graphDb , 'relationship', 'tagRel');
?>
<html>
<head>
	 <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"/> 
	<style>
		body, pre { 
			font-family: tahoma; 
			font-size: 12px;
		}
	</style>
</head>
<body>
<pre>
<?php
$bloggerUrl = "http://www.blogger.com/feeds/";

if ($profileID = $_GET['profileID']) {
	//Retrieving Profile
	if (@$xmlstr = file_get_contents($bloggerUrl."$profileID/blogs")) {
		$xml = new SimpleXMLElement($xmlstr);

	
		foreach ($xml->entry as $entry)
			$blogID[] = Blogger::findIdByUri($entry->link[3]->attributes()->href);
		
		$authorNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
		$authorNode->id = Blogger::findIdByUri($entry->author->uri);
		$authorNode->name = Blogger::normalize($entry->author->name);
		$authorNode->blogs = $blogID;
		$authorNode->save();
		
		echo "Author ({$authorNode->getId()}): $authorNode->id ($authorNode->name) with BlogID: ";
		
		echo "<ol>";
		foreach ($xml->entry->category as $cat) {
			$tagNode = new IndexNode($graphDb, $TagIndex, 'term');
			$tagNode->term = Blogger::normalize($cat->attributes()->term);
			$tagNode->save();
			$authorNode->createIndexRelationshipTo($TagRelIndex, $tagNode, 'Tag');
			echo "<li>{$tagNode->term}</li>";
		}
		echo "</ol>";
		
		//Retrieving BlogID Comments
		echo "<ul>";
		foreach ($blogID as $id) {
			if (@$xmlstr = file_get_contents($bloggerUrl."$id/comments/default")) {
				echo "<li>$id</li>";
				$commentAr = array();
				$xml = new SimpleXMLElement($xmlstr);
				foreach ($xml->entry as $entry) {
					if ($entry->author->uri) {
						$commentNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
						$commentNode->id = Blogger::findIdByUri($entry->author->uri);
						$commentNode->name = Blogger::normalize($entry->author->name);
						$commentNode->save();
						$commentNode->createRelationshipTo($authorNode, 'Comments');
						$commentAr[] = $commentNode->id . "(". $commentNode->name. ")" . "(". $commentNode->getId(). ")";
					}
				}
				echo "<ul><li>" . join('</li><li>',$commentAr) . "</li></ul>";
			}
		}
		echo "</ul>";
	} else { echo "Invalid profileID."; }
}
?>
</body>
</html>