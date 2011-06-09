<?php
require('neo4j.blog.php');
require('blogger.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
$AuthorsIndex = new IndexService( $graphDb , 'authors');
$TagIndex = new IndexService( $graphDb , 'tags');
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

//$ index -t Relationship --create AuthorTag 

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
		
		foreach ($xml->entry->category as $cat) {
			$tagNode = new IndexNode($graphDb, $TagIndex, 'term');
			$tagNode->term = Blogger::normalize($cat->attributes()->term);
			$tagNode->save();
			$relationship = $authorNode->createRelationshipTo($tagNode, 'Tag');
			$relationship->save();
		}

		echo "Author ({$authorNode->getId()}): $authorNode->id ($authorNode->name) with BlogID: ";
		//Retrieving BlogID Comments
		echo "<ul>";
		foreach ($blogID as $id) {
			if (@$xmlstr = file_get_contents($bloggerUrl."$id/comments/default")) {
				echo "<li>$id</li>";
				$commentID = array();
				$xml = new SimpleXMLElement($xmlstr);
				foreach ($xml->entry as $entry) {
					if ($entry->author->uri) {
						$commentNode = new IndexNode($graphDb, $AuthorsIndex, 'id');
						$commentNode->id = Blogger::findIdByUri($entry->author->uri);
						$commentNode->name = Blogger::normalize($entry->author->name);
						$commentNode->save();
						$relationship = $commentNode->createRelationshipTo($authorNode, 'Comments');
						$relationship->save();
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