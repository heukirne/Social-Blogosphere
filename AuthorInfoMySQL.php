<?php
function normalize($name) {
	$a = "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûýýþÿ/]%#\"?[^'"; 
	$b = 'AAAAAAACEEEEIIIIDNOOOOOOUUUUYbsaaaaaaaceeeeiiiidnoooooouuuyyby..........'; 		

	$string = $name;     
	$string = strtr($string, $a, $b); 
	return utf8_decode($string); 
}

// Google don't like flood requests
// 231 days for 2 million users profile page each 10s
$getAuthorInfo = "http://www.blogger.com/profile/";
$propArray = array('Local','Atividade','Signo_astrologico','Profissao','Sexo','Interesses','Introducao');
$link = mysql_connect('localhost', 'myself', 'myself');
if (!$link) {
    die('Could not connect: ' . mysql_error());
}
mysql_select_db('bloganalysis');
mysql_query("set wait_timeout = 7200");

$iA=0;
while (true) {

	$html="";
	$iA++;
	//echo ($iA).", ";
	
	$sql = "SELECT profileID as id FROM author WHERE Find = 0 ORDER BY RAND() LIMIT 1";
	$result = mysql_query($sql);
	if ($result) {
		$num_rows = mysql_num_rows($result);
		$author = mysql_fetch_assoc($result);
		mysql_free_result($result);
		if ($num_rows==0) break;
	} else {
		echo "\n".mysql_error()."\n";
		sleep(20);
		mysql_select_db('gemeos110');
		mysql_query("set wait_timeout = 7200");
		continue;
	}	

	if (@$html = file_get_contents($getAuthorInfo.$author['id'])) {
		$html = str_replace('strong','b',$html);
		preg_match_all("/=\"item-key\"\>([^<]*)<\/th>(\n)?([^<]*)(.*)/", $html, $listItens); 
		preg_match_all("/href=\"([^\"]+)\"[^\"]+\"contributor-to/", $html, $blogs); 	

		$prop = array();
		foreach ($listItens[1] as $i => $name) {
			$name = str_replace(':','',$name);
			$name = str_replace(' ','_',$name);
			$name = normalize(utf8_decode($name));
			$prop[$name] = $listItens[4][$i];
		}

		if (!empty($prop)) {
			foreach ($prop as $i => $value) {
				$result = array();
				if (strpos($value,'role')) preg_match("/ind=([^\"]*)/",$value,$result);
				if (strpos($value,'title')) preg_match("/q=([^\"]*)/",$value,$result);
				//if (strpos($value,'loc1')) preg_match("/loc1=(\w*)/",$value,$result);
				if (strpos($value,'favorites')) preg_match_all("/q=([^\"]*)/",$value,$result);
				if (strpos($i,'ocal')) preg_match("/loc0=(\w{2})/",$html,$result);
				if (strpos($value,'loc0')) preg_match("/loc0=(\w{2})/",$value,$result);
				
				if (!empty($result[1]) && is_array($result[1]))  {
					$prop[$i] = normalize(utf8_decode(urldecode(join(',',$result[1]))));
					$prop[$i] = normalize($prop[$i]);
				} else {
					$prop[$i] = normalize(empty($result)?$value:$result[1]);
				}
				$prop[$i] = str_replace('<td>','',$prop[$i]);
				$prop[$i] = str_replace('<.td>','',$prop[$i]);
				$prop[$i] = str_replace('<.tr>','',$prop[$i]);
			}
		}  
		
		//print_r($prop);
		//die();
	
		$sql = "UPDATE author SET ";
		foreach ($propArray as $name) {
			if (!empty($prop[$name])) {
				$sql .= "$name = '{$prop[$name]}', ";
			}
		}
		$sql .= " Blogs = '".implode(',',$blogs[1])."',";
		$sql .= " Find = 3";
		$sql .= " WHERE profileID = '{$author['id']}' limit 1;";		
		echo "+".$author['id'];
	} else {
		$erroHandle = error_get_last();
		if (strpos($erroHandle['message'],'404 Not Found')) {
			$sql = "UPDATE author SET Find = 404 WHERE profileID = '{$author['id']}';";
			echo "-";
		}
		if (strpos($erroHandle['message'],'500 Internal Server')) {
			$sql = "UPDATE author SET Find = 500 WHERE profileID = '{$author['id']}';";
			echo "-";
		}
		if (strpos($erroHandle['message'],'503')) {
			echo "#";
		}
	}
	//echo $sql;
	//die();
	mysql_query($sql);
	sleep(7);
	
}
mysql_close($link);
