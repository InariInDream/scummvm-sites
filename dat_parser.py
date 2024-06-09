import re
import os
import sys
from db_functions import db_insert, populate_matching_games

def remove_quotes(string):
    # Remove quotes from value if they are present
    if string and string[0] == "\"":
        string = string[1:-1]

    return string

def map_checksum_data(content_string):
    arr = {}
    temp = re.findall(r'("[^"]*")|\S+', content_string)

    for i in range(1, len(temp), 2):
        if i+1 < len(temp):
            if temp[i] == ')' or temp[i] in ['crc', 'sha1']:
                continue
            temp[i + 1] = remove_quotes(temp[i + 1])
            if temp[i + 1] == ')':
                temp[i + 1] = ""
            arr[temp[i]] = temp[i + 1].replace("\\", "")

    return arr

def map_key_values(content_string, arr):
    # Split by newline into different pairs
    temp = content_string.splitlines()

    # Add pairs to the dictionary if they are not parentheses
    for pair in temp:
        pair = pair.strip()
        if pair == "(" or pair == ")":
            continue
        pair = list(map(str.strip, pair.split(None, 1)))
        pair[1] = remove_quotes(pair[1])

        # Handle duplicate keys (if the key is rom) and add values to a array instead
        if pair[0] == "rom":
            if pair[0] in arr:
                arr[pair[0]].append(map_checksum_data(pair[1]))
            else:
                arr[pair[0]] = [map_checksum_data(pair[1])]
        else:
            arr[pair[0]] = pair[1].replace("\\", "")
            
    return arr
            
def match_outermost_brackets(input):
    """
    Parse DAT file and separate the contents each segment into an array
    Segments are of the form `scummvm ( )`, `game ( )` etc.
    """
    matches = []
    depth = 0
    inside_quotes = False
    cur_index = 0

    for i in range(len(input)):
        char = input[i]

        if char == '(' and not inside_quotes:
            if depth == 0:
                cur_index = i
            depth += 1
        elif char == ')' and not inside_quotes:
            depth -= 1
            if depth == 0:
                match = input[cur_index:i+1]
                matches.append((match, cur_index))
        elif char == '"' and input[i - 1] != '\\':
            inside_quotes = not inside_quotes

    return matches

def parse_dat(dat_filepath):
    """
    Take DAT filepath as input and return parsed data in the form of
    associated arrays
    """
    if not os.path.isfile(dat_filepath):
        print("File not readable")
        return

    with open(dat_filepath, "r") as dat_file:
        content = dat_file.read()

    header = {}
    game_data = []
    resources = {}

    matches = match_outermost_brackets(content)
    if matches:
        for data_segment in matches:
            if "clrmamepro" in content[data_segment[1] - 11: data_segment[1]] or \
                "scummvm" in content[data_segment[1] - 8: data_segment[1]]:
                map_key_values(data_segment[0], header)
            elif "game" in content[data_segment[1] - 5: data_segment[1]]:
                temp = {}
                map_key_values(data_segment[0], temp)
                game_data.append(temp)
            elif "resource" in content[data_segment[1] - 9: data_segment[1]]:
                temp = {}
                map_key_values(data_segment[0], temp)
                resources[temp["name"]] = temp

    return header, game_data, resources, dat_filepath

# Process command line args
if "--upload" in sys.argv:
    index = sys.argv.index("--upload")
    for filepath in sys.argv[index + 1:]:
        if filepath == "--match":
            continue
        db_insert(parse_dat(filepath))

if "--match" in sys.argv:
    populate_matching_games()