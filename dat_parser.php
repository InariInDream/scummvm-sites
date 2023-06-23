<?php

ini_set('memory_limit', '512M');

/**
 * Convert string of checksum data from rom into associated array
 * Returns array instead of updating one like map_key_values
 */
function map_checksum_data($content_string) {
  $arr = array();
  $temp = preg_split('/("[^"]*")|\h+/', $content_string, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);

  for ($i = 1; $i < count($temp); $i += 2) {
    if ($temp[$i] == ')')
      continue;
    if ($temp[$i + 1][0] == '"') {
      $temp[$i + 1] = substr($temp[$i + 1], 1, -1);
    }
    $arr[$temp[$i]] = stripslashes($temp[$i + 1]);
  }

  return $arr;
}

/**
 * Convert string as received by regex parsing to associated array
 */
function map_key_values($content_string, &$arr) {

  // Split by newline into different pairs
  $temp = preg_split("/\r\n|\n|\r/", $content_string);

  // Add pairs to the associated array if they are not parantheses
  foreach ($temp as $pair) {
    if (trim($pair) == "(" or trim($pair) == ")")
      continue;
    $pair = array_map("trim", preg_split("/ +/", $pair, 2));

    // Remove quotes from value if they are present
    if ($pair[1][0] == "\"")
      $pair[1] = substr($pair[1], 1, -1);

    // Handle duplicate keys (if the key is rom) and add values to a arary instead
    if ($pair[0] == "rom") {
      if (array_key_exists($pair[0], $arr)) {
        array_push($arr[$pair[0]], map_checksum_data($pair[1]));
      }
      else {
        $arr[$pair[0]] = array(map_checksum_data($pair[1]));
      }
    }
    else {
      $arr[$pair[0]] = stripslashes($pair[1]);
    }
  }
}

/**
 * Parse DAT file and separate the contents each segment into an array
 * Segments are of the form `scummvm ( )`, `game ( )` etc.
 */
function match_outermost_brackets($input) {
  $matches = array();
  $depth = 0;
  $inside_quotes = false;
  $cur_index = 0;

  for ($i = 0; $i < strlen($input); $i++) {
    $char = $input[$i];

    if ($char == '(' && !$inside_quotes) {
      if ($depth === 0) {
        $cur_index = $i;
      }
      $depth++;
    }
    elseif ($char == ')' && !$inside_quotes) {
      $depth--;
      if ($depth === 0) {
        $match = substr($input, $cur_index, $i - $cur_index + 1);
        array_push($matches, array($match, $cur_index));
      }
    }
    elseif ($char == '"' && $input[$i - 1] != '\\') {
      $inside_quotes = !$inside_quotes;
    }
  }

  return $matches;
}

/**
 * Take DAT filepath as input and return parsed data in the form of
 * associated arrays
 */
function parse_dat($dat_filepath) {
  $dat_file = fopen($dat_filepath, "r") or die("Unable to open file!");
  $content = fread($dat_file, filesize($dat_filepath));
  fclose($dat_file);

  if (!$content) {
    error_log("File not readable");
  }

  $header = array();
  $game_data = array();
  $resources = array();

  $matches = match_outermost_brackets($content);
  if ($matches) {
    foreach ($matches as $data_segment) {
      if (strpos(substr($content, $data_segment[1] - 11, 11), "clrmamepro") !== false ||
        strpos(substr($content, $data_segment[1] - 8, 8), "scummvm") !== false) {
        map_key_values($data_segment[0], $header);
      }
      elseif (strpos(substr($content, $data_segment[1] - 5, $data_segment[1]), "game") !== false) {
        $temp = array();
        map_key_values($data_segment[0], $temp);
        array_push($game_data, $temp);
      }
      elseif (strpos(substr($content, $data_segment[1] - 9, $data_segment[1]), "resource") !== false) {
        map_key_values($data_segment[0], $resources);
      }
    }

  }

  // Print statements for debugging
  // Uncomment to see parsed data

  // echo "<pre>";
  // print_r($header);
  // print_r($game_data);
  // print_r($resources);
  // echo "</pre>";

  return array($header, $game_data, $resources);
}

/**
 * Routine for inserting a game into the database, inserting into engine and
 * game tables
 */
function insert_game($engineid, $title, $gameid, $extra, $platform, $lang, $conn) {
  // Set @engine_last if engine already present in table
  $exists = false;
  if ($res = $conn->query(sprintf("SELECT id FROM engine WHERE engineid = '%s'", $engineid))) {
    if ($res->num_rows > 0) {
      $exists = true;
      $conn->query(sprintf("SET @engine_last = '%d'", $res->fetch_array()[0]));
    }
  }

  // Insert into table if not present
  if (!$exists) {
    $query = sprintf("INSERT INTO engine (name, engineid)
  VALUES (NULL, '%s')", $engineid);
    $conn->query($query);
    $conn->query("SET @engine_last = LAST_INSERT_ID()");
  }

  // Insert into game
  $query = sprintf("INSERT INTO game (name, engine, gameid, extra, platform, language)
  VALUES ('%s', @engine_last, '%s', '%s', '%s', '%s')", mysqli_real_escape_string($conn, $title),
    $gameid, mysqli_real_escape_string($conn, $extra), $platform, $lang);
  $conn->query($query);
  $conn->query("SET @game_last = LAST_INSERT_ID()");
}

/**
 * Inserting new fileset
 * Called for both detection entries and other forms of DATs
 */
function insert_fileset($src, $key, $detection, $conn) {
  // $status can be: {detection, unconfirmed, confirmed (unused here)}
  $status = "unconfirmed";
  $game = "NULL";

  if ($detection) {
    $status = "detection";
    $game = "@game_last";
  }

  // $game should not be parsed as a mysql string, hence no quotes
  $query = sprintf("INSERT INTO fileset (game, status, src, `key`)
  VALUES (%s, '%s', '%s', '%s')", $game, $status, $src, $key);
  $conn->query($query);
  $conn->query("SET @fileset_last = LAST_INSERT_ID()");
}

/**
 * Routine for inserting a file into the database, inserting into all
 * required tables
 * $file is an associated array (the contents of 'rom')
 * If checksum of the given checktype doesn't exists, silently fails
 */
function insert_file($file, $detection, $conn, $checktype = "md5") {
  // Find first checksum value

  $checksum = "";
  foreach ($file as $key => $value) {
    if ($key != "name" && $key != "size")
      $checksum = $value;
  }

  $query = sprintf("INSERT INTO file (name, size, checksum, fileset, detection)
  VALUES ('%s', '%s', '%s', @fileset_last, %d)", mysqli_real_escape_string($conn, $file["name"]),
    $file["size"], $checksum, $detection);
  $conn->query($query);
  $conn->query("SET @file_last = LAST_INSERT_ID()");
}

function insert_filechecksum($file, $checktype, $conn) {
  if (!array_key_exists($checktype, $file))
    return;

  $checksize = 0;
  $checksum = $file[$checktype];
  if (strpos($checktype, '-') !== false) {
    $checksize = explode('-', $checktype)[1];
    $checktype = explode('-', $checktype)[0];
  }

  if (strpos($checksum, ':') !== false) {
    $checktype .= "-" . explode(':', $checksum)[0];
    $checksum = explode(':', $checksum)[1];
  }

  $query = sprintf("INSERT INTO filechecksum (file, checksize, checktype, checksum)
  VALUES (@file_last, '%s', '%s', '%s')", $checksize, $checktype, $checksum);
  $conn->query($query);
}

/**
 * Insert values from the associated array into the DB
 * They will be inserted under gameid NULL as the game itself is unconfirmed
 */
function db_insert($data_arr) {
  $header = $data_arr[0];
  $game_data = $data_arr[1];
  $resources = $data_arr[2];

  $servername = "localhost";
  $username = "username";
  $password = "password";
  $dbname = "integrity";

  // Create connection
  mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
  $conn = new mysqli($servername, $username, $password);
  $conn->set_charset('utf8mb4');
  $conn->autocommit(FALSE);

  // Check connection
  if ($conn->connect_errno) {
    die("Connect failed: " . $conn->connect_error);
  }

  $conn->query("USE " . $dbname);

  /**
   * Author can be:
   *  scummvm -> Detection Entries
   *  scanner -> CLI tool
   *  _anything else_ -> DAT file
   */
  $author = $header["author"];

  /**
   * src can be:
   *  detection -> Detection entries (source of truth)
   *  user -> Submitted by users via ScummVM, unmatched (Not used in the parser)
   *  scan -> Submitted by scanner, unmatched
   *  dat -> Submitted by DAT, unmatched
   *  partialmatch -> Submitted by DAT, matched
   *  fullmatch -> Submitted by scanner, matched
   */
  $src = "";
  if ($author == "scanner")
    $src = "scan";
  elseif ($author == "scummvm")
    $src = "detection";
  else
    $src = "dat";

  foreach ($game_data as $fileset) {
    if ($src == "detection") {
      $engineid = $fileset["sourcefile"];
      $gameid = $fileset["name"];
      $title = $fileset["title"];
      $extra = $fileset["extra"];
      $platform = $fileset["platform"];
      $lang = $fileset["language"];

      insert_game($engineid, $title, $gameid, $extra, $platform, $lang, $conn);
    }

    $key = NULL;
    insert_fileset($src, $key, ($src == "detection"), $conn);
    foreach ($fileset["rom"] as $file) {
      insert_file($file, ($src == "detection"), $conn, "md5-5000");
      insert_filechecksum($file, "md5-5000", $conn);
    }
  }
  if (!$conn->commit())
    echo "Inserting failed<br/>";
}

// db_insert(parse_dat("ngi.dat"));

?>
