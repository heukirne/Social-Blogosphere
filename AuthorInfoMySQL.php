<?php
// Google don't like flood requests
// 231 days for 2 million users profile page each 10s
require('blogger.php');
$getAuthorInfo = "http://www.blogger.com/profile/";
$propArray = array('Atividade','Signo_astrologico','Profissao','Sexo');
$link = mysql_connect('localhost', 'root', '');
mysql_select_db('gemeos110');
if (!$link) {
    die('Could not connect: ' . mysql_error());
}

$iA=0;
while (true) {

	$sql = "SELECT profileID as id FROM author WHERE Find = 0 and Local = 'BR' ORDER BY RAND() LIMIT 1";
	$result = mysql_query($sql);
	$author = mysql_fetch_assoc($result);
	mysql_free_result($result);
	
	if (!$author) break;
	
	$html="";
	echo ($iA++).", ";
	
	if ($html = file_get_contents($getAuthorInfo.$author['id'])) {
		$html = str_replace('strong','b',$html);
		preg_match_all("/<b>([^<]*)<\/b>(\n)?([^<]*)(.*)/", $html, $listItens); 
		preg_match_all("/href=\"([^\"]+)\"[^\"]+\"contributor-to/", $html, $blogs); 
	
		$prop = array();
		foreach ($listItens[1] as $i => $name) 
			if (strpos($name,':')) {
				$name = str_replace(':','',$name);
				$name = str_replace(' ','_',$name);
				$name = Blogger::normalize($name);
				$prop[$name] = trim($listItens[3][$i])?$listItens[3][$i]:$listItens[4][$i];
			}
		if (!empty($prop)) {
			foreach ($prop as $i => $value) {
				$result = array();
				if (strpos($value,'role')) preg_match("/ind=([^\"]*)/",$value,$result);
				if (strpos($value,'title')) preg_match("/q=([^\"]*)/",$value,$result);
				if (strpos($value,'loc1')) preg_match("/loc1=(\w*)/",$value,$result);
				if (strpos($value,'loc2')) preg_match("/loc2=(\w*)/",$value,$result);
				if (strpos($value,'loc0')) preg_match("/loc0=(\w{2})/",$value,$result);
				$prop[$i] = Blogger::normalize((empty($result))?$value:$result[1]);
			}
		} 
			
		$sql = "UPDATE author SET ";
		foreach ($propArray as $name) {
			if (!empty($prop[$name])) {
				$sql .= "$name = '{$prop[$name]}', ";
			}
		}
		$sql .= " Blogs = '".implode(',',$blogs[1])."',";
		$sql .= " Find = 1";
		$sql .= " WHERE profileID = '{$author['id']}';";		
		
	} else {
		$erroHandle = error_get_last();
		if (strpos($erroHandle['message'],'404 Not Found') || strpos($erroHandle['message'],'500 Internal Server')) {
			$sql = "UPDATE author SET Find = 1 WHERE profileID = '{$author['id']}';";			
		}
	}
	mysql_query($sql);
	sleep(7);
	
}
mysql_close($link);