<?php

class Blogger
{
	public static function findIdByUri($uri) {
		preg_match("/\d+/", $uri, $id);
		return $id[0];
	}	
	
	public static function normalize($name) {
		$a = 'ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ‗אבגדהוזחטיךכלםמןנסעףפץצרשתû‎‎‏ÿ/]'; 
		$b = 'AAAAAAACEEEEIIIIDNOOOOOOUUUUYbsaaaaaaaceeeeiiiidnoooooouuuyyby..'; 		
		$string = utf8_decode($name);     
		$string = strtr($string, $a, $b); 
		return utf8_encode($string); 
	}
}


?>