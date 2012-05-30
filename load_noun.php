<?php

//MongoCursor::timeout = -1; 
$m = new Mongo("mongodb://localhost:27017");
$db = $m->blogdb;
$c_words = $db->words;

$set = array('$set' => array("subst" => 1));

$homepage = file_get_contents('substantivos.txt');
$arWords = preg_split("/\n/", $homepage);

foreach ($arWords as $word) {
	$word = iconv("UTF-8", "ASCII//TRANSLIT//IGNORE", $word);
	$word = preg_replace("/\W/", "", $word);
	$o_words = array("_id" => $word);
	$c_words->update($o_words,$set,false);
}

?>