import json
import requests 
import os
from postgre_con import query_postgre

def get_RPGIs():
    RGPIs = query_postgre("games", ["RiotPlatformGameId"])
    return [tuple[0] for tuple in RGPIs]

def get_game_timeline(id_game):
    gameTimeline = json.loads(requests.get(f"https://lol.fandom.com/api.php?action=query&format=json&prop=revisions&titles=V5%20data%3A{id_game}%2FTimeline&rvprop=content&rvslots=main").content)
    gameTimeline = json.dumps(gameTimeline)
    gameTimeline = gameTimeline.replace(' ', '')
    gameTimeline = gameTimeline.replace('\\n', '')
    gameTimeline = json.loads(gameTimeline)
    for y in gameTimeline['query']['pages']:
        gameTimeline = gameTimeline['query']['pages'][y]['revisions'][0]['slots']['main']['*']
    gameTimeline = json.loads(gameTimeline)

    return gameTimeline

def register_new_JSON():
    for platform_game_id in get_RPGIs():
        if not os.path.isfile(f'game_json/{platform_game_id}.json'):
            print(platform_game_id)
            game_timeline = get_game_timeline(platform_game_id) #test it out for one single game

            with open(f'game_json/{platform_game_id}.json', 'w') as file:
                json.dump(game_timeline, file)