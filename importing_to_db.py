import requests
import json 
import pandas as pd
import mysql.connector #wrapper connecting to mysql database
from mysql.connector import errorcode
import os 
from dotenv import load_dotenv

load_dotenv()

youtube_stats = pd.read_csv("connection.csv")
youtube_stats.drop(columns=youtube_stats.columns[0], axis = 1, inplace=True)
user = os.getenv('USER_DB')
password = os.getenv('PASS_DB')
host = os.getenv('HOST_DB')
db_name = os.getenv('DB_NAME')
try:
    connection = mysql.connector.connect(user = user, password = password, host = host, database = db_name)
except mysql.connector.OperationalError as e:
    raise e
else:
    print("You are connected to the database")


#1 - creating table
TABLES = {}
TABLES["youtube_stats"] = (
    "CREATE TABLE IF NOT EXISTS `youtube_stats` ("
    "`vid_id` varchar(11) NOT NULL,"
    "`view_count` int(11) NOT NULL,"
    "`like_count` int(11) NOT NULL,"
    "`fav_count` int(10) NOT NULL,"
    "`com_count` int(11) NOT NULL,"
    "`title` varchar(100) NOT NULL,"
    "PRIMARY KEY (`vid_id`)"
") ENGINE=InnoDB")

cursor = connection.cursor()

table_description = TABLES["youtube_stats"]
try:
    print("Creating table {}: ".format("youtube_stats"), end = '')
    cursor.execute(table_description)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("table already exists")
    else:
        print(err.msg)
else:
    print("working fine")


#Determining if a video exists
def is_id_exists(curr, vid_id):
    if_exists = ("SELECT vid_id FROM youtube_stats WHERE vid_id = %s")
    curr.execute(if_exists, (vid_id,))

    return curr.fetchone() is not None

def row_update(curr, vid_id, view_count, like_count, fav_count, com_count, title):
    query = ('''UPDATE youtube_stats
        SET view_count = %s,
        like_count = %s,
        fav_count = %s,
        com_count = %s,
        title = %s
        WHERE vid_id = %s;''')
    curr.execute(query, (view_count, like_count, fav_count, com_count, title, vid_id))

def append_row(curr, vid_id, view_count, like_count, fav_count, com_count, title):
    query = ('''INSERT INTO youtube_stats(vid_id, view_count, like_count, fav_count, com_count, title)
             VALUES (%s, %s, %s, %s, %s, %s);''')
    curr.execute(query, (vid_id, view_count, like_count, fav_count, com_count, title))

""" def update_db(curr, df):
    tmp_df = pd.DataFrame(columns=['vid_id', 'view_count', 'like_count','fav_count','com_count'])
    for i, row in youtube_stats.iterrows():
        if is_id_exists(cursor, row['vid_id']):
            row_update(cursor, row["vid_id"], row["view_count"], row["like_count"], row["fav_count"], row["com_count"])
        else:
            tmp_df = pd.concat([tmp_df, row], axis = 0)
    return tmp_df
new_vid_db = update_db(cursor, youtube_stats) """

def update_db(curr, df):
    for i, row in df.iterrows():
        if is_id_exists(curr, row['vid_id']):
            row_update(curr, row["vid_id"], row["view_count"], row["like_count"], row["fav_count"], row["com_count"], row["title"])
        else:
            append_row(curr, row["vid_id"], row["view_count"], row["like_count"], row["fav_count"], row["com_count"], row["title"])

update_db(cursor, youtube_stats)
connection.commit()




cursor.close()
connection.close()