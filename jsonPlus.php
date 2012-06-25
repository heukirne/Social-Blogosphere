<?php
require_once 'google-api-php-client/src/apiClient.php';
require_once 'google-api-php-client/src/contrib/apiPlusService.php';

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
$client->setDeveloperKey('');
$service = new apiPlusService($client);

while (1) {

$sql = "SELECT plusID FROM author WHERE plusID is not null and blogs = '' LIMIT 10";
$result = mysql_query($sql);
if (mysql_num_rows($result) < 10) { sleep(60); continue; }
while($row = mysql_fetch_assoc($result)) {

//echo "\n({$row['plusID']}) : ";
try {
   $plus = $service->people->get($row['plusID']);
} catch (Exception $e) {
   echo $e->getCode();
   if ($e->getCode()==403) die('die');
   else {
	$sql = "UPDATE author SET blogs = 'none' WHERE plusID = '{$row['plusID']}' LIMIT 1; ";
	mysql_query($sql);
   }
   continue;
}

//Tratamentos
$plus['displayName'] = preg_replace("/'/","",utf8_decode($plus['displayName']));
if (!empty($plus['aboutMe'])) $plus['aboutMe'] = preg_replace("/'/","",utf8_decode($plus['aboutMe']));
$plus['blogs'] = array();

foreach($plus['urls'] as $url)
   if (!isset($url['type']))
      $plus['blogs'][] = preg_replace("/'/","",$url['value']);
if (is_array($plus['blogs'])) $plus['blogs'] = implode(',',$plus['blogs']);
if (empty($plus['blogs']))  $plus['blogs'] = 'none';

//Monta SQL
$sql = "UPDATE author SET nome = '{$plus['displayName']}' ";
if (!empty($plus['gender'])) $sql .= ", Sexo = '{$plus['gender']}'";
if (!empty($plus['aboutMe'])) $sql .= ", Introducao = '{$plus['aboutMe']}'";
$sql .= ", blogs = '{$plus['blogs']}'";
$sql .= " WHERE plusID ='{$plus['id']}' and blogs = '' LIMIT 1;";

//echo $sql."\n";
echo "+";
if (!mysql_query($sql))
   echo mysql_error();
//sleep(0.2);

}
}

echo "end";
mysql_free_result($result);
mysql_close($link);
