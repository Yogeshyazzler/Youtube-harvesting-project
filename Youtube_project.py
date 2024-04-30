import pandas as pd
from googleapiclient.discovery import build
import pymongo
import streamlit as st
from mysql.connector import connect
import mysql.connector
import pandas as pd
import datetime
from datetime import timedelta
from datetime import time
import re
from datetime import timedelta
import isodate
from dateutil.parser import isoparse
from isodate import parse_duration



def api_connect():
    api_id='AIzaSyB1SaoRH6J4x5lKvTtEPH4lj16bWanmy44'
    api_service_name ='youtube'
    api_version = 'v3'
    youtube=build(api_service_name,api_version,developerKey=api_id)

    return youtube 

youtube=api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(part="snippet,contentDetails,statistics",
                                    id=channel_id
                                    )
    response=request.execute()


    for i in response['items']:
        data=dict(channel_name=i['snippet']['title'],
                channel_id=i['id'],
                subscribers=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


def get_video_ids(channel_id):

    Video_ids=[]
    response1=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()

    playlist_id=response1['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None

    while True:

        request2= youtube.playlistItems().list(
                                                part="snippet",
                                                playlistId=playlist_id,
                                                maxResults=5,
                                                pageToken=next_page_token)

        response3 = request2.execute()

        for i in range (len(response3["items"])):
                Video_ids.append(response3["items"][i]["snippet"]["resourceId"]["videoId"])
        
        next_page_token=response3.get("nextPageToken")

        if next_page_token is None:
            break
    return Video_ids


def get_video_info(video_id_info):
    
    videos_list=[]

    for video_id in video_id_info:
        request=youtube.videos().list(
        part="snippet,ContentDetails,statistics",
        id=video_id
        )
        response4=request.execute() 

        for item in response4["items"]:
            data=dict(channel_name=item["snippet"]['channelTitle'],
                    channel_id=item["snippet"]["channelId"],
                    video_id=item["id"],
                    title=item["snippet"]["title"],
                    tags=item["snippet"].get("tags"),
                    thumb_nail=item["snippet"]["thumbnails"]["default"]["url"],
                    descripition=item["snippet"].get("description"),
                    published_date=item["snippet"]["publishedAt"],
                    duration=item["contentDetails"]["duration"],
                    views=item["statistics"].get("viewCount"),
                    likes=item["statistics"].get("likeCount"),
                    comments=item["statistics"].get("commentCount"),
                    favourite=item["statistics"]["favoriteCount"],
                    definition=item["contentDetails"]["definition"],
                    caption_status=item["contentDetails"]["caption"])
            videos_list.append(data)
            
    return videos_list


def get_comment_info(video_ids1):

    comment_data=[]
    try:
        for video_id1 in video_ids1:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id1,
                maxResults=50,
                pageToken=None
            )

            response5=request.execute()

            for item in response5["items"]:
                data=dict(comment_id=item["snippet"]["topLevelComment"]["id"],
                        video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        comment_published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_data.append(data)
                
    except:
        pass
    return comment_data

def get_playlist(channel_id):

    all_data=[]
    next_page_token=None
    while True:
        request=youtube.playlists().list(
            part="snippet,ContentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token)
            
        response6=request.execute()

        for item in response6["items"]:
            data=dict(playlist_id=item["id"],
                    title=item["snippet"]["title"],
                    channel_id=item["snippet"]["channelId"],
                    channel_name=item["snippet"]["channelTitle"],
                    published_At=item["snippet"]["publishedAt"],
                    video_count=item["contentDetails"]["itemCount"])
            all_data.append(data)
        next_page_token=response6.get("nextPageToken")
        if next_page_token is None:
            break
    return all_data

client=pymongo.MongoClient("mongodb://yogeshyazzler:Poki1234@ac-k6143ye-shard-00-00.s277tr3.mongodb.net:27017,ac-k6143ye-shard-00-01.s277tr3.mongodb.net:27017,ac-k6143ye-shard-00-02.s277tr3.mongodb.net:27017/?ssl=true&replicaSet=atlas-5hd3a3-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
db=client["youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll_1=db["channel_details"]
    coll_1.insert_one({"channel_information":ch_details,"playlist_details":pl_details,"video_Information":vi_details,"comment_details":com_details})

    return "upload completed successfully"

def channels_table(channel_name):
    mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="root",
                                database="youtube",
                                port="3306"
                                )
    cursor=mydb.cursor()
    
    
    create_channel='''create table if not exists channels(
        channel_name varchar(255),
        channel_id varchar(255) primary key,
        subscribers bigint,
        views bigint,
        Total_videos int,
        channel_description text,
        playlist_id varchar(255)

    )'''
    cursor.execute(create_channel)
    mydb.commit()
       
    single_channel=[]
    ch_list=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for ch_data in coll_1.find({"channel_information.channel_name":channel_name},{"_id":0}):
        single_channel.append(ch_data["channel_information"]) 

    df_single_channel=pd.DataFrame(single_channel)
    

    for index,row in df_single_channel.iterrows():
        insert_query='''insert into channels(channel_name,channel_id,subscribers,views,Total_videos,channel_description,playlist_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values=(row["channel_name"],
                row["channel_id"],
                row["subscribers"],
                row["views"],
                row["Total_videos"],
                row["channel_description"],
                row["playlist_id"])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
             exists= f"{channel_name} channel already exist"
             return exists


def playlist_table(channel_name):
    mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="root",
                                database="youtube",
                                port="3306"
                                )
    cursor=mydb.cursor()

    
    create_playlist='''create table if not exists playlists(
        playlist_id varchar(255) primary key,
        title varchar(255) ,
        channel_id varchar(255),
        channel_name varchar(255),
        published_At datetime,
        video_count int

    )'''

    cursor.execute(create_playlist)
    print("playlist created")
    mydb.commit()

    single_playlist=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for ch_data in coll_1.find({"channel_information.channel_name":channel_name},{"_id":0}):
        single_playlist.append(ch_data["playlist_details"]) 

    df_single_playlist=pd.DataFrame(single_playlist[0])

    for index,row in df_single_playlist.iterrows():
        insert_query='''insert into playlists(playlist_id,title,channel_id,channel_name,published_At,video_count)
                        VALUES (%s, %s, %s, %s, %s, %s)'''
        values=(row["playlist_id"],
                row["title"],
                row["channel_id"],
                row["channel_name"],
                datetime.datetime.strptime(row['published_At'],'%Y-%m-%dT%H:%M:%SZ'),
                row["video_count"])
   
        cursor.execute(insert_query,values)
        mydb.commit()
        print("execute successfully")

def videos_table(channel_name):

    mydb = mysql.connector.connect(host="127.0.0.1",
                                user="root",
                                password="root",
                                database="youtube",
                                port="3306"
                                )
    cursor=mydb.cursor()

    create_videos='''create table if not exists videos(
                    channel_name varchar(255),
                    channel_id varchar(255),
                    video_id varchar(255) primary key,
                    title varchar(255),
                    tags text,
                    thumb_nail varchar(255),
                    descripition text,
                    published_date datetime,
                    duration time,
                    views bigint,
                    likes bigint,
                    comments int,
                    favourite int,
                    definition varchar(10),
                    caption_status varchar(255)

    )'''

    cursor.execute(create_videos)
    mydb.commit()

    single_video=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for ch_data in coll_1.find({"channel_information.channel_name":channel_name},{"_id":0}):
        single_video.append(ch_data["video_Information"]) 

    df_single_video=pd.DataFrame(single_video[0])

    df_single_video['tags'] = df_single_video['tags'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)

    for index,row in df_single_video.iterrows():
            du = isodate.parse_duration(row['duration'])
            duration_seconds = int(du.total_seconds())
            formatted_duration = '{:02}:{:02}:{:02}'.format(
            duration_seconds // 3600,
            (duration_seconds % 3600) // 60,
            duration_seconds % 60
            )
            duration_time = datetime.datetime.strptime(formatted_duration, '%H:%M:%S').time()
            insert_query='''insert into videos(channel_name,channel_id,video_id,title,tags,thumb_nail,descripition,published_date,duration,views,likes,comments,favourite,
                    definition,caption_status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            
            values=(row["channel_name"],
                    row["channel_id"],
                    row["video_id"],
                    row["title"],
                    row["tags"],
                    row["thumb_nail"],
                    row["descripition"],
                    datetime.datetime.strptime(row['published_date'],'%Y-%m-%dT%H:%M:%SZ'),
                    duration_time,
                    row["views"],
                    row["likes"],
                    row["comments"],
                    row["favourite"],
                    row["definition"],
                    row["caption_status"])
    
            cursor.execute(insert_query,values)
            mydb.commit()
            print("execute successfully") 

def comments_table(channel_name):

    mydb = mysql.connector.connect(host="127.0.0.1",
                            user="root",
                            password="root",
                            database="youtube",
                            port="3306"
                            )
    cursor=mydb.cursor()

    create_comments='''create table if not exists comments(
    comment_id varchar(255) primary key,
    video_id varchar(255) ,
    comment_text text,
    comment_author varchar(255),
    comment_published datetime
    )'''

    cursor.execute(create_comments)
    mydb.commit()

    single_comment=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for ch_data in coll_1.find({"channel_information.channel_name":channel_name},{"_id":0}):
        single_comment.append(ch_data["comment_details"]) 

    df_single_comment=pd.DataFrame(single_comment[0])

    for index,row in df_single_comment.iterrows():
        insert_query='''insert into comments(comment_id,video_id,comment_text,comment_author,comment_published)
                VALUES (%s, %s, %s, %s, %s)'''
        values=(row["comment_id"],
               row["video_id"],
               row["comment_text"],
               row["comment_author"],  
               datetime.datetime.strptime(row['comment_published'],'%Y-%m-%dT%H:%M:%SZ')
        )

        cursor.execute(insert_query,values)
        mydb.commit() 

def tables(channel):

    exists=channels_table(channel)

    if exists:
        return exists
    else:
        playlist_table(channel)
        videos_table(channel)
        comments_table(channel)
        return "Tables Created"

def show_channels():
    ch_list=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]

    for ch_data in coll_1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df0=st.dataframe(ch_list)

    return df0

def show_playlist():    
    pl_list=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for pl_data in coll_1.find({},{"_id":0,"playlist_details":1}):
        for i in range (len(pl_data["playlist_details"])):
            pl_list.append(pl_data["playlist_details"][i])

    df1=st.dataframe(pl_list)

    return df1

def show_videos():
    vid_list=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for vid_data in coll_1.find({},{"_id":0,"video_Information":1}):
        for i in range (len(vid_data["video_Information"])):
            vid_list.append(vid_data["video_Information"][i])

    df2=st.dataframe(vid_list) 

    return df2

def show_comments(): 
    cmt_list=[]
    db=client["youtube_data"]
    coll_1=db["channel_details"]
    for cmt_data in coll_1.find({},{"_id":0,"comment_details":1}):
        for i in range (len(cmt_data["comment_details"])):
            cmt_list.append(cmt_data["comment_details"][i])

    df3=st.dataframe(cmt_list)

st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

if 'sidebar_state' not in st.session_state:
    st.session_state.sidebar_state = True 

if st.sidebar.title('Skills Learned'):
    st.session_state.sidebar_state = not st.session_state.sidebar_state


import streamlit as st

# Define functions to display messages
def display_python_scripting_message():
    if st.session_state.python_scripting_message_visible:
        st.sidebar.write('Utilized python functions,list,dictionary.')
        st.sidebar.write('Packages used - Pandas')
    else:
        st.sidebar.empty()

def display_data_collections_message():
    if st.session_state.data_collections_message_visible:
        st.sidebar.write('Data collected from Youtube channels')
    else:
        st.sidebar.empty()

def display_mongodb_message():
    if st.session_state.mongodb_message_visible:
        st.sidebar.write('Data collected from Youtube channesl are stored in Mongo DB')
        st.sidebar.write('Packages used - Pymongos')
    else:
        st.sidebar.empty()

def display_api_integration_message():
    if st.session_state.api_integration_message_visible:
        st.sidebar.write('Youtube Api is fetched from developer google')
    else:
        st.sidebar.empty()

def display_data_management_message():
    if st.session_state.data_management_message_visible:
        st.sidebar.write('The data in mongo db are moved to Mysql for Data Analysis and finding Insights')
    else:
        st.sidebar.empty()
def display_summary_message():
    if st.session_state.data_summary_message_visible:
        st.sidebar.write('Youtube-Data-Harvesting-And-Warehousing YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and analyse data from numerous YouTube channels. SQL, MongoDB, and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.')
    else:
        st.sidebar.empty()

# Track the previous and current stages
previous_stage = None
current_stage = None

# Initialize session state variables
if 'python_scripting_message_visible' not in st.session_state:
    st.session_state.python_scripting_message_visible = False
if 'data_collections_message_visible' not in st.session_state:
    st.session_state.data_collections_message_visible = False
if 'mongodb_message_visible' not in st.session_state:
    st.session_state.mongodb_message_visible = False
if 'api_integration_message_visible' not in st.session_state:
    st.session_state.api_integration_message_visible = False
if 'data_management_message_visible' not in st.session_state:
    st.session_state.data_management_message_visible = False
if 'data_summary_message_visible' not in st.session_state:
    st.session_state.data_summary_message_visible = False

# Buttons for each caption
if st.sidebar.button("Python scripting"):
    if current_stage == "Python scripting":
        previous_stage, current_stage = None, None
    else:
        previous_stage, current_stage = current_stage, "Python scripting"
        st.session_state.python_scripting_message_visible = not st.session_state.python_scripting_message_visible
        display_python_scripting_message()

if st.sidebar.button("Data collections"):
    if current_stage == "Data collections":
        previous_stage, current_stage = None, None
    else:
        previous_stage, current_stage = current_stage, "Data collections"
        st.session_state.data_collections_message_visible = not st.session_state.data_collections_message_visible
        display_data_collections_message()

if st.sidebar.button("MongoDB"):
    if current_stage == "MongoDB":
        previous_stage, current_stage = None, None
    else:
        previous_stage, current_stage = current_stage, "MongoDB"
        st.session_state.mongodb_message_visible = not st.session_state.mongodb_message_visible
        display_mongodb_message()

if st.sidebar.button("API Integration"):
    if current_stage == "API Integration":
        previous_stage, current_stage = None, None
    else:
        previous_stage, current_stage = current_stage, "API Integration"
        st.session_state.api_integration_message_visible = not st.session_state.api_integration_message_visible
        display_api_integration_message()

if st.sidebar.button("Data Management"):
    if current_stage == "Data Management":
        previous_stage, current_stage = None, None
    else:
        previous_stage, current_stage = current_stage, "Data Management"
        st.session_state.data_management_message_visible = not st.session_state.data_management_message_visible
        display_data_management_message()

if st.sidebar.button("Summary"):
    if current_stage == "Summary":
        previous_stage, current_stage = None, None
    else:
        previous_stage, current_stage = current_stage, "Summary"
        st.session_state.data_summary_message_visible= not st.session_state.data_summary_message_visible
        display_summary_message()
    


channel_id=st.text_input("Enter the channel ID")

if st.button ("collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
        
    if channel_id in ch_ids:
        st.success("channel already exist")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

all_channels=[]
db=client["youtube_data"]
coll_1=db["channel_details"]
for ch_data in coll_1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]["channel_name"])

unique_channel=st.selectbox("select the Channel",all_channels)

if st.button("Insert to Mysql"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("select the table for view",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channels() 

elif show_table=="Playlists":
    show_playlist()

elif show_table=="Videos":
    show_videos()

elif show_table=="Comments":
    show_comments()


mydb = mysql.connector.connect(host="127.0.0.1",
                        user="root",
                        password="root",
                        database="youtube", 
                        port="3306"
                        )
cursor=mydb.cursor()

question=st.selectbox("select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question == "1.What are the names of all the videos and their corresponding channels?":
    query1='''select title,channel_name from videos '''
    cursor.execute(query1)

    t1=cursor.fetchall()

    q1=pd.DataFrame(t1,columns=["videos","channel"])

    st.write(q1)

elif question == "2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name,Total_videos from youtube.channels where Total_videos = (select max(Total_videos) from youtube.channels)'''
    cursor.execute(query2)

    t2=cursor.fetchall()

    q2=pd.DataFrame(t2,columns=["channel_name","Total_videos"])

    st.write(q2)

elif question == "3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select video_id,title,views,channel_name from videos order by views desc limit 10;'''
    cursor.execute(query3)

    t3=cursor.fetchall()

    q3=pd.DataFrame(t3,columns=["video_id","title","views","channel_name"])

    st.write(q3) 

elif question == "4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments,title as video_title from videos;'''
    cursor.execute(query4)

    t4=cursor.fetchall()

    q4=pd.DataFrame(t4,columns=["comments","title"])

    st.write(q4)

elif question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select likes as Highest_likes,title as videotitle,channel_name from videos order by likes desc limit 1;'''
    cursor.execute(query5)

    t5=cursor.fetchall()

    q5=pd.DataFrame(t5,columns=["Highest_likes","video_title","channel_name"])

    st.write(q5)

elif question == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select likes as likecount,title as video_title from videos;'''
    cursor.execute(query6)

    t6=cursor.fetchall()

    q6=pd.DataFrame(t6,columns=["likecount","video_title"])

    st.write(q6)

elif question == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name,sum(views) as total_views from videos group by channel_name;'''
    cursor.execute(query7)

    t7=cursor.fetchall()

    q7=pd.DataFrame(t7,columns=["channel_name","total_views"])

    st.write(q7)

elif question == "8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select video_id as video_title,channel_name,published_date from videos where year(published_date)=2022;'''
    cursor.execute(query8)

    t8=cursor.fetchall()

    q8=pd.DataFrame(t8,columns=["video_title","channel_name","published_date"])
    
    st.write(q8)

elif question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''SELECT channel_name, round(AVG(TIME_TO_SEC(duration))/60,2) Average_duration_minutes FROM  videos GROUP BY channel_name;'''
    cursor.execute(query9)

    t9=cursor.fetchall()

    q9=pd.DataFrame(t9,columns=["channel_name","Average_duration_minutes"])

    T9=[]
    for index,row in q9.iterrows():
        channel_title=row["channel_name"]
        average_duration_min=row["Average_duration_minutes"]
        average_duration_str=str(average_duration_min)
        T9.append(dict(channel_title=channel_title,average_duration_min=average_duration_str)) 
    x9=pd.DataFrame(T9)

    st.write(x9) 

elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select c.video_id,count(c.comment_id) as Highest_comment,v.channel_name from comments as c join videos as v on 
                c.video_id=v.video_id group by c.video_id order by count(c.comment_id) desc limit 1;'''
    cursor.execute(query10)

    t10=cursor.fetchall()

    q10=pd.DataFrame(t10,columns=["video_id","Highest_comment","channel_name"])
    
    st.write(q10)




