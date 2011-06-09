<?php
require('php-neo-rest.php');
$graphDb = new GraphDatabaseService('http://localhost:7474/db/data/');
?>
<html>
<head>
	 <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/> 
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
if ($user = $_GET['user']) {
	$authorNode = $graphDb->createNode();
	$authorNode->name = $user;
	$authorNode->save();
	if (@$html = file_get_contents("http://$user.wordpress.com/2011/page/54/")) {
		preg_match_all("/comments-link[^-]*-sep[^:]*:\/\/([^#]*)[^>]*>(\d+)/", $html, $links); 
		echo "http://".$links[1][2]."\n";
		if (@$html = file_get_contents("http://".$links[1][2])) {
			preg_match_all("/comment-author-(\w+)/", $html, $comments); 
			preg_match_all("/http:\/\/gravatar.com\/(\w+)\"/", $html, $likes); 
			echo "Comentários:\n";
			print_r($comments[1]);
			foreach($comments[1] as $commentUser) {
				$commentNode = $graphDb->createNode();
				$commentNode->name = $commentUser;
				$commentNode->save();
				$relationship = $commentNode->createRelationshipTo($authorNode, 'Comments');
				$relationship->save();
			}
			echo "Likes:\n";
			print_r($likes[1]);
			foreach($likes[1] as $likeUser) {
				$likeNode = $graphDb->createNode();
				$likeNode->name = $likeUser;
				$likeNode->save();
				$relationship = $likeNode->createRelationshipTo($authorNode, 'Like');
				$relationship->save();
			}
		}
	} 
}
?>
</body>
</html>