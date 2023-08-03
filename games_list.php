<?php
require $_SERVER['DOCUMENT_ROOT'] . '/include/pagination.php';

$filename = "games_list.php";
$records_table = "game";
$select_query = "SELECT engineid, gameid, extra, platform, language, game.name,
status, fileset.id as fileset
FROM game
JOIN engine ON engine.id = game.engine
JOIN fileset ON game.id = fileset.game";
$order = "ORDER BY gameid";

// Filter column => table
$filters = array(
  "engineid" => "engine",
  "gameid" => "game",
  "extra" => "game",
  "platform" => "game",
  "language" => "game",
  "name" => "game",
  "status" => "fileset"
);

$mapping = array(
  'engine.id' => 'game.engine',
  'game.id' => 'fileset.game',
);

create_page($filename, 25, $records_table, $select_query, $order, $filters, $mapping);
?>

