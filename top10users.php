<?php

MongoCursor::timeout = -1; 
$m = new Mongo("mongodb://localhost:27017");
$db = $m->blogdb;

$word = 'politica';
$map = "function(){ " .
					" if (this.content) " .
					"	if (this.content.indexOf('" . $word . "')>0) {".
					"		idAuthor = this.authorID;".
					"		this.comments.forEach ( function (comment) { ".
					"			emit ( comment.authorID , 1 ); " .
					"			emit ( idAuthor , 1 ); " .
					"		} ); ".
					"	} ".
					"}; ";							

$reduce = "function( key , values ){ ".
					"	var total = 0; " .
					"	for ( var i=0; i<values.length; i++ ) {".
					"		total += values[i]; ".
					"   } " .
					"	return total; ".
					"};";	

$query = array("comments" => array( '$ne' => array()), "content" => array('$ne' => ""));
$top10 = $db->command(array(
    "mapreduce" => "posts", 
    "map" => $map,
    "reduce" => $reduce,
    "query" => $query,
    "out" => array("replace" => "top10authors")));

$sort = array("value" => -1);
$top10list = $db->selectCollection($top10['result'])->find()->sort($sort)->limit(10);
print_r($top10list);

foreach ($top10list as $author) {
	print_r($author);
}

?>