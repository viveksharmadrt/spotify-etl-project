from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd
import sqlalchemy
import requests
import datetime
import json
import sqlite3

## Creating database to store our songs details
DATABASE_LOCATION = "sqlite:///my_played_song_tracks.sqlite"

## Provide AuthToken of spotify

## Reading credentials from config file
with open('/path/of/configfile/config.json') as file:
    params=json.load(file)['params']

USER_ID = params['user_id']
TOKEN = params['auth_token']

def check_dataframe(df: pd.DataFrame) -> bool:
    # Checking whether dataframe is empty
    if df.empty:
        print("dataframe is empty")
        return False

    # Checking unique key for songs
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Songs are duplicate")   

    # Checking for null value
    if df.isnull().values.any():
        raise Exception("Found empty value")

    # Checking song for yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df['timestamps'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("Any one song is today's song")

    return True

if __name__ == "__main__":

    ############### Extraction Step for ETL Process ###############

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    ## Time parameter to pull data after or before
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    ## Uncomment and place epoch to test
    #yesterday_unix_timestamp = '<epoch>'

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()
    #print(data)

## Create empty list
song_names = []
artist_names = []
movie_name = []
played_at_list = []
timestamps = []

## Iterate through the json
for songs in data['items']:
    song_names.append(songs['track']['name'])
    movie_name.append(songs['track']['album']['name'])
    artist_names.append(songs['track']['album']['artists'][0]['name'])
    played_at_list.append(songs['played_at'])
    timestamps.append(songs['played_at'][0:10])

## Create dictionary
songs_dict = {'song_names':song_names, 'movie_name':movie_name, 'artist_names':artist_names, 'played_at':played_at_list, 'timestamps':timestamps}

## Create pandas dataframe
songs_df = pd.DataFrame(data=songs_dict)

############### Transformation step (Validation step) ###############
if check_dataframe(songs_df):
    print("Validation completed")

############### Loading Data into database ###############

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_song_tracks.sqlite')
cursor = conn.cursor()

sql_query = '''CREATE TABLE IF NOT EXISTS my_played_song_tracks(song_names VARCHAR(200), movie_name VARCHAR(200), artist_names VARCHAR(200), played_at VARCHAR(200), timestamps VARCHAR(200), CONSTRAINT primary_key_constraint PRIMARY KEY (played_at))'''

print("chcek")
cursor.execute(sql_query)
print("Database opened")   

try:
    songs_df.to_sql("my_played_song_tracks", engine, index=False, if_exists='append')
except:
    print("Data already exists in the database")
    
conn.close()
print("Close database successfully")
