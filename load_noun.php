<?php

//MongoCursor::timeout = -1; 
$m = new Mongo("mongodb://localhost:27018");
$db = $m->blogdb;
$c_words = $db->wordCount;

$set = array('$set' => array("subst" => 1));

$homepage = file_get_contents('word_net_subst');
$arWords = preg_split("/\n/", $homepage);

foreach ($arWords as $wordT) {
   if ($wordT[0] == 'n') {
	$wordT = preg_replace("/n\d+\s=\s/",'',$wordT);
	$arWords2 = preg_split("/,|_/", $wordT);
	foreach ($arWords2 as $word) {
	   $word = iconv("UTF-8", "ASCII//TRANSLIT//IGNORE", $word);
	   $word = preg_replace("/\W/", "", $word);
           echo $word.", ";
	   $o_words = array("_id" => $word);
	   $c_words->update($o_words,$set);
	}
   }
}

?>
