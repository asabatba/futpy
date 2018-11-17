
from time import sleep
from bs4 import BeautifulSoup as bs
import requests
import csv
import datetime
import sqlite3
import concurrent.futures
import queue

# db = sqlite3.connect('futdb.db')

proxies = {
    # 'http': 'http://10.110.8.42:8080',
    # 'https': 'http://10.110.8.42:8080',
}

retry = 0
while (retry < 100):
    try:
        csvfile = open('players_'+str(retry)+'.csv', 'w',
                       encoding='utf-8', newline='')
    except:
        retry += 1
    else:

        break


def request_retry(url, retries=6, wait=0.5, **kwargs):

    for i in range(retries):
        try:
            r = requests.get(url, **kwargs)  # proxies!!
        except Exception as err:
            print("*Try {}/{} has failed ({})".format(i+1, retries, err))
            sleep(1/20+i*wait)
        else:
            return r

    raise ConnectionError('{} connection retries have failed.'.format(retries))


def parse_player(player_href, team_name):
    write_all_this = list()
    player_dict = dict()

    # player_href = player_html.find("a", "name").get("href")
    # print(player_href)

    r_player = request_retry("http://futmondo.com"+player_href)

    soup_player = bs(r_player.text, 'html.parser')
    player_name = soup_player.head.title.get_text().split(":")[0].strip()
    value = soup_player.find("ul", class_="value").span.get_text().split(" ")[
        0].replace(".", "").strip()
    role = soup_player.find("div", class_="pos")["class"][-1][2:]
    # print("role: "+role)

    player_dict['name'] = player_name
    player_dict['team'] = team_name
    player_dict['role'] = role

    # db.execute(
    #     "INSERT OR REPLACE INTO players(name,team_id,role) VALUES( '{}', {}, '{}' );".format(player_name, team_id, role))
    # player_id = db.execute(
    #     "SELECT player_id FROM players WHERE name='{}';".format(player_name)).fetchone()[0]

    print("[{}] {}".format(team_name, player_name))
    matches = soup_player.find_all("ul", class_="playerStats")

    for match in matches:
        # print(match.get_text())
        # if ("yes" in match['class']):
        week = match.find("li", class_="gweek").get_text().split(
            " ")[-1].strip()
        print(week)
        score = match.find("li", class_="press").get_text().strip()
        # player_dict['scores'][int(week)] = int(score)

        # print("- "+week+": " +
        #         score)
        played = ('yes' in match.find("li", class_="played")['class'])
        titular = ('yes' in match.find("li", class_="titular")['class'])

        player_dict['j'] = int(week)
        if played:
            player_dict['score'] = int(score)
        else:
            player_dict['score'] = None
        player_dict['played'] = int(played)
        player_dict['titular'] = int(titular)

        for jscript in soup_player.find_all("script"):
            if ("drawChart" in jscript.get_text()):
                jscript_lines = jscript.get_text().split("\n")
                for jline in jscript_lines:
                    if ("new Date" in jline):
                        jline = jline.split(",")
                        short_date = datetime.datetime.fromtimestamp(
                            int(jline[0].split("(")[1].split(")")[0])/1000).strftime("%Y-%m-%d")
                        # short_date_nh = datetime.datetime.fromtimestamp(
                        #     int(jline[0].split("(")[1].split(")")[0])/1000).strftime("%Y%m%d")
                        player_dict['date'] = short_date
                        value = int(1000000*float(jline[1].strip()))
                        player_dict['value'] = value
                        break
                        # db.execute(
                        #     "INSERT OR REPLACE INTO player_values(player_id,date,value) VALUES( {}, date('{}'), {} );".format(player_id, short_date, value))
                        # print(jline)
                break
        write_all_this.append(player_dict)
        # writer.writerow(player_dict)
        
        # print(player_dict)

        # print("\tplayed: "+str(played)+"\ttitular: "+str(titular))
    # print(write_all_this)
    # raise SyntaxError()
    return write_all_this


def parse_team(team_html):
    team_href = "http://www.futmondo.com"+team_html.a.get("href")

    player_list = list()

    r_team = request_retry(team_href)

    soup_team = bs(r_team.text, 'html.parser')

    li_players = soup_team.find("ul", "ulPlayers").find_all("li")

    for li in li_players:
        player_href = li.find("a", "name").get("href")
        player_list.append(player_href)

    team_name = soup_team.head.title.get_text().split(":")[0].strip()
    return player_list, team_name


###  START   ###

fieldnames = ['name', 'team', 'role', 'date', 'value',
              'j', 'score', 'played', 'titular']
writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

writer.writeheader()


# r = requests.get(
#     'http://www.futmondo.com/team?team=52038563b8d07d930b00008a', proxies=proxies)

r = request_retry('http://www.futmondo.com/team?team=52038563b8d07d930b00008a')

soup_all = bs(r.text, 'html.parser')
teams = soup_all.find("div", class_="teamCrests").find_all(
    "div", class_="teamLink")
player_team_list = list()

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures_t = {executor.submit(parse_team, div): div for div in teams}
    for future in concurrent.futures.as_completed(futures_t):
        li_add, team_name = future.result()
        for player in li_add:
            player_team_list.append((player, team_name))

print("Get teams -> Done")

with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
    futures_p = {executor.submit(
        parse_player, player[0], player[1]): player for player in player_team_list}
    for future in concurrent.futures.as_completed(futures_p):
        all_lines = future.result()
        # print(all_lines)
        for line in all_lines:
            writer.writerow(line)

csvfile.close()
# db.close()
