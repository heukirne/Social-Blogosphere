<?php
// Google don't like flood requests
// 41 days for 2 million user in pages of 25 each 30s and 50% probability
$local = 'BR';
$getAtuhorBR = "http://www.blogger.com/profile-find.g?t=l&loc0=$local&start=:i&ct=:ct";
$link = mysql_connect('localhost', 'gemeos110', 'none');
mysql_select_db('gemeos110');
if (!$link) {
    die('Could not connect: ' . mysql_error());
}
mysql_query("set wait_timeout = 7200");

while (true) {
	$ct = array(1 => array(0 => ""));
	for ($i=0;$i<4000;$i+=10) {

			$urlNow = str_replace(':i',$i,$getAtuhorBR);
			$urlNow = str_replace(':ct',$ct[1][0],$urlNow);
			
			if (@$html = file_get_contents($urlNow)) {			
				preg_match_all("/<h2><a href=\"http:\/\/www.blogger.com\/profile\/(\d+)/", $html, $listProfile); 
				if (empty($ct[1][0])) preg_match_all("/ct=([^\"]+)/", $html, $ct);
				
				$bNew = false;
				foreach ($listProfile[1] as $id) {
					$sql = "INSERT INTO author SET profileID='$id' , Local = '$local';";
					mysql_query($sql);
					if (!mysql_error()) $bNew=true;
				}
				
				if ($bNew) echo "+";
				else  echo "-";
				
			} else {
				$erroHandle = error_get_last();
				if (strpos($erroHandle['message'],'404 Not Found')) {
					echo "-";
				}
				if (strpos($erroHandle['message'],'500 Internal Server')) {
					echo "-";
				}
				if (strpos($erroHandle['message'],'503')) {
					echo "#";
				}
			}
			
			sleep(7);
	}
}
mysql_close($link);
