<?php
require_once 'google-api-php-client/src/apiClient.php';
require_once 'google-api-php-client/src/contrib/apiBloggerService.php';

$configFile = file_get_contents('Class/my_config.properties');
preg_match_all("/\n([^=]*)=(.*)/", $configFile, $config);

$link = mysql_connect($config[2][1], $config[2][2], $config[2][3]);
mysql_select_db('bloganalysis');
if (!$link) {
    die('Could not connect: ' . mysql_error());
}
mysql_query("set wait_timeout = 7200");

$client = new apiClient();
//$client->setUseObjects(true);
$client->setDeveloperKey('{API-KEY}');
$service = new apiBloggerService($client);

while (1) {

$sql = "SELECT blogID FROM blogs WHERE url is null ORDER BY RAND() LIMIT 1";
$result = mysql_query($sql);
while($row = mysql_fetch_assoc($result)) {

try {
   $blog = $service->blogs->get($row['blogID']);
} catch (Exception $e) {
   echo $e->getCode();
   if ($e->getCode()==403 || $e->getCode()==400) die('die');
   else {
	$sql = "UPDATE blogs SET url = '' WHERE blogID = '{$row['blogID']}' LIMIT 1; ";
	mysql_query($sql);
   }
   continue;
}

//Tratamentos
$blog['published'] = strtotime($blog['published']);
$blog['published'] = date('Y-m-d h:i:s', $blog['published']);
$blog['updated'] = strtotime($blog['updated']);
$blog['updated'] = date('Y-m-d h:i:s', $blog['updated']);

$blog['description'] = preg_replace("/'/","",utf8_decode($blog['description']));
$blog['name'] = preg_replace("/'/","",utf8_decode($blog['name']));

//Monta SQL
$sql = "UPDATE blogs SET name = '{$blog['name']}', description = '{$blog['description']}', ";
$sql .= "published = '{$blog['published']}', updated = '{$blog['updated']}', url = '{$blog['url']}', ";
$sql .= "totalPosts = {$blog['posts']['totalItems']}, ";
$sql .= "language = '{$blog['locale']['language']}', country = '{$blog['locale']['country']}' ";
$sql .= ", variant = '{$blog['locale']['variant']}'";
$sql .= " WHERE blogID ='{$blog['id']}' LIMIT 1;";

//echo $sql."\n";
echo "+";
mysql_query($sql);
sleep(5);

}
}

mysql_free_result($result);

$sql = "SELECT count(*) as count FROM blogs WHERE url is not null;";
$result = mysql_query($sql);
$cont = mysql_fetch_assoc($result);
mysql_free_result($result);

echo "\nTotal:".$cont['count']."\n";

mysql_close($link);
