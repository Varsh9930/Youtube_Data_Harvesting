from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import certifi
import psycopg2
import pandas as pd
import streamlit as st

#Connecting using API key
def connect_API():
    API_id = "AIzaSyApgcpuZII_lTtjx0uqKtmF7HW5UfD8K78"
    API_Serv_Name = "Youtube"
    API_version = "v3"
    youtube = build(API_Serv_Name,API_version,developerKey=API_id)
    return youtube

youtube = connect_API()

# getting channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
                        part="snippet,ContentDetails,statistics",
                        id = channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_Name = i["snippet"]["title"],
                Channel_Id = i["id"],
                Subscribers = i["statistics"]['subscriberCount'],
                Total_Views = i['statistics']['viewCount'],
                Total_Videos = i['statistics']['videoCount'],
                Channel_description = i['snippet']['description'],
                Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#getting Video IDs
def get_video_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id = channel_id,
                                    part = 'contentDetails').execute()
    #Playlist_Id = ['contentDetails']['relatedPlaylists']['uploads']
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                part = "snippet",
                                                playlistId = Playlist_Id,
                                                maxResults = 50,
                                                pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']["resourceId"]['videoId'])
        next_page_token = response1.get('nextPageToken') 
        
        if next_page_token is None:
            break  
    return video_ids    

#getting video information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet, contentDetails, statistics",
            id = video_id
        )
        response = request.execute()
        
        for item in response["items"]:    
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes_count = item['statistics'].get('likeCount'),
                        Comments_count = item['statistics'].get('commentCount'), 
                        Favourite_Counts = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data
            
            
#getting comment information
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Authur_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
                
    except:
        pass            
    return Comment_data

#getting playist details

def get_playlist_details(channel_id):
        next_page_token = None
        Total_Data = []
        while True:
                request = youtube.playlists().list(
                        part = 'snippet, ContentDetails',
                        channelId = channel_id,
                        maxResults = 50, 
                        pageToken = next_page_token
                )
                response = request.execute()

                for item in response['items']:
                        data = dict(Playlist_Id = item['id'],
                                Title = item['snippet']['title'],
                                Channel_Id = item['snippet']['channelId'],
                                Channel_Name = item['snippet']['channelTitle'],
                                Published_Date = item['snippet']['publishedAt'],
                                No_of_Videos = item['contentDetails']['itemCount'])
                        Total_Data.append(data)
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                        break
        return Total_Data
                
        
#connecting to MongoDB, importing required packages      

ca = certifi.where()

client = pymongo.MongoClient("mongodb+srv://Varshdk:sonumonu@varshini.ajc2es6.mongodb.net/?retryWrites=true&w=majority",tlsCAFile=ca)
db = client["Youtube_data"]

def channel_details(channel_id):
    Ch_details = get_channel_info(channel_id)
    playlist_details = get_playlist_details(channel_id)
    Vi_ids = get_video_ids(channel_id)
    Video_details = get_video_info(Vi_ids)
    comment_details = get_comment_info(Vi_ids)
    
    collection1 = db["channel_details"]
    collection1.insert_one({"channel_information":Ch_details, "playlist_information":playlist_details,
                            "video_information":Video_details, "comment_information":comment_details})
    
    return "upload completed successfully"

# creating table for channels, playlists, videos and comments
def channel_table():
    my_db = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sonu9monu#",
                            database = "youtube_data",
                            port = "5432")
    cursor = my_db.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    my_db.commit()

    try:
        create_query ='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Total_Views bigint,
                                                            Total_Videos int,
                                                            Channel_description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        my_db.commit()
        
        
    except:
        print("Channels table already created")
        
        
    ch_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Total_Views,
                                                Total_Videos,
                                                Channel_description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row["Subscribers"],
                row["Total_Views"],
                row["Total_Videos"],
                row["Channel_description"],
                row["Playlist_Id"])
        try:
            cursor.execute(insert_query,values)
            my_db.commit()
            
            
        except:
            print("Channels values are already inserted")
            
#Creating Playlist Table
def playlist_table():
    my_db = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Sonu9monu#",
                                database = "youtube_data",
                                port = "5432")
    cursor = my_db.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    my_db.commit()

    create_query ='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            Published_Date timestamp,
                                                            No_of_Videos int
                                                            )'''
                                                            
        
        
    cursor.execute(create_query)
    my_db.commit()
    
    
    pl_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for pl_data in collection1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    
    for index,row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published_Date,
                                                No_of_Videos)
                                                                                             
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Published_Date'],
                row['No_of_Videos']
        )
        
        cursor.execute(insert_query,values)
        my_db.commit()   
        
        
#Creating Videos Table

def videos_table():
    my_db = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sonu9monu#",
                            database = "youtube_data",
                            port = "5432")
    cursor = my_db.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    my_db.commit()

    create_query ='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(80) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes_count bigint,
                                                    Comments_count int, 
                                                    Favourite_Counts int,
                                                    Definition varchar(20),
                                                    Caption_Status varchar(50)
                                                                                    )'''
                                                            
    cursor.execute(create_query)
    my_db.commit()

    vi_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for vi_data in collection1.find({},{"_id":0, "video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
    for index,row in df2.iterrows():
            insert_query = '''insert into videos(Channel_Name,
                                                        Channel_Id ,
                                                        Video_Id ,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes_count,
                                                        Comments_count, 
                                                        Favourite_Counts,
                                                        Definition,
                                                        Caption_Status)
                                                                                             
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                    
            
                                                
            values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes_count'],
                    row['Comments_count'],
                    row['Favourite_Counts'],
                    row['Definition'],
                    row['Caption_Status']
            )
            
            cursor.execute(insert_query,values)
            my_db.commit()   
            
#Creating Comments Table
def comments_table():
    my_db = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Sonu9monu#",
                                database = "youtube_data",
                                port = "5432")
    cursor = my_db.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    my_db.commit()

    create_query ='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                            Video_Id varchar(50),
                                            Comment_Text text,
                                            Comment_Authur_name varchar(150),
                                            Comment_Published timestamp)'''
                                                            
        
        
    cursor.execute(create_query)
    my_db.commit()

    com_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for com_data in collection1.find({},{"_id":0, "comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Authur_name,
                                                    Comment_Published)
                                                                                                
                                                    values(%s,%s,%s,%s,%s)'''

            values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Authur_name'],
                    row['Comment_Published']
                    )
            
            cursor.execute(insert_query,values)
            my_db.commit()   
            
def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()
    return "Tables Created Successfully"

def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)
    
    return df

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for pl_data in collection1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    
    return df1

def show_videos_tables():
    vi_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for vi_data in collection1.find({},{"_id":0, "video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for com_data in collection1.find({},{"_id":0, "comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    
    return df3    


#streamlit part
st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
with st.sidebar:
    st.header("Skills Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id = st.text_input("Enter the chennel ID")

#just incase not to insert the same channel ID while running the mongodb code
if st.button("collect and store data"):
    ch_ids = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
    if channel_id in ch_ids:
        st.success('Channel Details of the given channel Id already exists')
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)    
        
        
if st.button("Migrate to Sql"):
    Table = tables()
    st.success(Table)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))    


if show_table == "CHANNELS":
    show_channels_table()

elif show_table == "PLAYLISTS":
    show_playlists_table() 
    
elif show_table == "VIDEOS":
    show_videos_tables()
    
elif show_table == "COMMENTS":
    show_comments_table()   


#SQL connection
my_db = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "Sonu9monu#",
                        database = "youtube_data",
                        port = "5432")
cursor = my_db.cursor()

question = st.selectbox("Select your question", ("1. Names of all the videos and the channel name",
                                                 "2. Channels with most number of videos and the number of videos",
                                                "3. Top 10 most viewed videos and their channels",
                                                "4. Number of Comments on each video and their video name",
                                                "5. Videos with highest number of likes and its channel name",
                                                "6. Total number of likes and dislikes on each video, and their video names",
                                                "7. Total number of views for each channel and its channel name",
                                                "8. Names of all the channels that have published videos in the year 2022",
                                                "9. Average duration of all videos in each channel and its channel names",
                                                "10. Videos with highest number of comments, and its channel names"))

if question == "1. Names of all the videos and the channel name":
    query1 = '''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    my_db.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns = ["Title of the video", "Channel name"])
    st.write(df)
    
elif question == "2. Channels with most number of videos and the number of videos":
    query2 = '''select channel_name as channelname, total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    my_db.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns = ["Channel name", "No of videos"])
    st.write(df2)
    
elif question == "3. Top 10 most viewed videos and their channels":
    query3 = '''select views as views, title as videotitle, channel_name as channelname from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    my_db.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns = ["Views", "Video title", "Channel name"])
    st.write(df3)
    
elif question == "4. Number of Comments on each video and their video name":
    query4 = '''select Comments_count as no_comments, title as videotitle from videos where Comments_count is not null'''
    cursor.execute(query4)
    my_db.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns = ["No of Comments","Video title"])
    st.write(df4)
    
elif question == "5. Videos with highest number of likes and its channel name":
    query5 = '''select Likes_count as likecount, title as videotitle, channel_name as channelname
                from videos where Likes_count is not null order by Likes_count desc'''
    cursor.execute(query5)
    my_db.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns = ["No of likes", "Video title", "Channel name"])
    st.write(df5)
    
elif question == "6. Total number of likes and dislikes on each video, and their video names":
    query6 = '''select Likes_count as likecount, title as videotitle from videos'''
    cursor.execute(query6)
    my_db.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns = ["No of likes", "Video title"])
    st.write(df6)
    
elif question == "7. Total number of views for each channel and its channel name":
    query7 = '''select channel_name as channelname, Total_views as views from channels'''
    cursor.execute(query7)
    my_db.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns = ["channel name","Total No of views"])
    st.write(df7)
    
    
elif question == "8. Names of all the channels that have published videos in the year 2022":
    query8 = '''select channel_name as channelname, Published_Date as videoreleased, title as video_title from videos
                where extract(year from Published_Date)=2022'''
    cursor.execute(query8)
    my_db.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns = ["Channel name","Published Date","Title of the video"])
    st.write(df8)


elif question == "9. Average duration of all videos in each channel and its channel names":
    query9 = '''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    my_db.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns = ["Channel name","Average Duration"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["Channel name"]
        average_duration = row["Average Duration"]
        average_duration_str = str(average_duration)
        
        T9.append(dict(channeltitle = channel_title, avgduration = average_duration_str))
        
    df1 = pd.DataFrame(T9)
    st.write(df1)


elif question == "10. Videos with highest number of comments, and its channel names":
    query10 = '''select Comments_count as comments, channel_name as channelname, title as videotitle from videos where
            Comments_count is not null order by Comments_count desc'''
    cursor.execute(query10)
    my_db.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns = ["No of Comments","Channel name","Title of the video"])
    st.write(df10)
    