#!/usr/bin/env python
# coding: utf-8

# In[1]:


from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st


# In[2]:


def Api_connect():
  api_service_name = "youtube"
  api_version = "v3"

  key="AIzaSyAsUr8GK5CALcjV-Cc5a0gT7_22iu2gtoU"

  youtube=build(api_service_name, api_version, developerKey=key)

  return youtube
youtube=Api_connect()


# In[3]:


#get channel deatils by channel id
def get_channel_info(channel_id):
  request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
  response=request.execute()
  for i in response["items"]:
    data=dict(channel_Name=i["snippet"]["title"],
              channel_Id=i["id"],
              subscribers=i["statistics"]["subscriberCount"],
              views=i["statistics"]["viewCount"],
              total_Videos=i["statistics"]["videoCount"],
              channel_Description=i["snippet"]["description"],
              playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


# In[4]:


#give channel id in () and run the shell to get channel deatils

#get_channel_info("UCISj5L-H88jOZZQWm4wdXAw")


# In[5]:


#get all video ids by channel id
def get_video_ids(channel_Id):
  video_ids=[]
  response = youtube.channels().list(part="contentDetails",
                                    id=channel_Id).execute()
  playlist_Id=response["items"][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token=None

  while True:
    response1 = youtube.playlistItems().list(
                                              part='snippet',
                                              playlistId=playlist_Id,
                                              maxResults=50,
                                              pageToken=next_page_token).execute()
    for i in range(len(response1["items"])):
      video_ids.append(response1["items"][i]["snippet"]["resourceId"]['videoId'])
    next_page_token=response1.get("nextPageToken")
    if next_page_token is None:
      break
  return video_ids


# In[6]:


#give channel id in () and run the shell to get video ids and it append in Video_Ids list

#Video_Ids=get_video_ids('UCISj5L-H88jOZZQWm4wdXAw')


# In[7]:


#get video deatils by video ids
def get_video_info(video_ids):
  video_data=[]
  for video_id in video_ids:
    request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response=request.execute()

    for item in response["items"]:
      data=dict(channel_Name=item['snippet']['channelTitle'],
                channel_Id=item['snippet']["channelId"],
                video_id=item["id"],
                title=item["snippet"]["title"],
                tags=item["snippet"].get("tags"),
                thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                description=item["snippet"].get("description"),
                published_date=item["snippet"]['publishedAt'],
                duration=item['contentDetails']['duration'],
                views=item['statistics'].get('viewCount'),
                likes=item['statistics'].get('likeCount'),
                comments=item['statistics'].get('commentCount'),
                favoriteCount=item['statistics']['favoriteCount'],
                definition=item['contentDetails']['definition'],
                caption_status=item['contentDetails']['caption']
                )
      video_data.append(data)
  return video_data


# In[8]:


#give videos ids list in () and run the shell to get video details

#get_video_info(Video_Ids)


# In[9]:


#get comment deatils
def get_comment_info(video_ids):
  nextpagetoken=None
  comment_data=[]
  try:
    for video_id in video_ids:
      while True:
        request=youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50,
            pageToken=nextpagetoken)
        response=request.execute()

        for item in response["items"]:
          data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                    video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
          comment_data.append(data)
        nextpagetoken=response.get('nextPageToken')
        if nextpagetoken is None:
          break
  except:
    pass
  return comment_data


# In[10]:


##give video ids list in () and run the shell to get comment details

#get_comment_info(Video_Ids)


# In[11]:


#get playlist details
def get_playlist_details(channel_id):
  next_page_token=None
  playlist_data=[]
  while True:
    request=youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=50,
        pageToken=next_page_token
    )
    response=request.execute()

    for item in response["items"]:
      data=dict(playlist_id=item["id"],
                title=item['snippet']['title'],
                channel_id=item['snippet']['channelId'],
                channel_name=item['snippet']['channelTitle'],
                publishedat=item['snippet']['publishedAt'],
                video_count=item['contentDetails']['itemCount']
              )
      playlist_data.append(data)
    next_page_token=response.get('nextPageToken')
    if next_page_token is None:
        break

  return playlist_data


# In[12]:


##give channel id in () and run the shell to get playlist details

#get_playlist_details('UCISj5L-H88jOZZQWm4wdXAw')


# In[13]:


#connect with mongo db

client=pymongo.MongoClient('mongodb://localhost:27017')
db=client["youtube_data"]


# In[14]:


def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  play_details=get_playlist_details(channel_id)
  vi_ids=get_video_ids(channel_id)
  vi_details=get_video_info(vi_ids)
  cmt_details=get_comment_info(vi_ids)

  coll=db["channel_details"]
  coll.insert_one({"channel_information":ch_details,"playlist_information":play_details,
                   "video_information":vi_details,"comment_information":cmt_details})
  return "success"


# In[15]:


#above sheel is the code to get youtube channel data by give it id only and upload into mongodb


# In[16]:


#run below comment to upload data into mangodb
#channel_details("UCIFQgj1Rhx-FFgyo0zzPSfw")


# In[17]:


#connect with mysql
def Channels_table():
    myconnection = pymysql.connect(host='localhost',user='root',passwd='Soundar@2003',database='youtubedata')
    cur = myconnection.cursor()
    drop_query='''drop table if exists channels'''
    cur.execute(drop_query)
    myconnection.commit()

    try:
        cur.execute('''create table channels(channel_Name text,
                    channel_Id varchar(80) primary key,
                    subscribers bigint,
                    views bigint,
                    total_Videos int,
                    channel_Description text,
                    playlist_Id text)''')
    except:
        print("table already created")



    #convert mongodb data into dataframe

    ch_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for ch_data in  coll.find({},{"_id":0,"channel_information":1},):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)



    #insert the values of dataframe into sql

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_Name,channel_Id,subscribers,views,total_Videos,channel_Description,playlist_Id) values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
               row['channel_Id'],
               row['subscribers'],
               row['views'],
               row['total_Videos'],
               row['channel_Description'],
               row['playlist_Id'])
        try:
            cur.execute(insert_query,values)
            myconnection.commit()
        except:
            print("Channels already inserted")


# In[18]:


def playlist_table():

    #connect with mysql
    
    myconnection = pymysql.connect(host='localhost',user='root',passwd='Soundar@2003',database='youtubedata')
    cur = myconnection.cursor()


    # create table for playlist

    drop_query='''drop table if exists playlists'''
    cur.execute(drop_query)
    myconnection.commit()

    try:
        cur.execute('''create table playlists(playlist_id varchar(100) primary key,
                    title varchar(100),
                    channel_id varchar(100),
                    channel_name varchar(100),
                    publishedat text,
                    video_count int)''')
    except:
        print("table already created")

    #convert mongodb data into dataframe

    pl_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for pl_data in  coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])

    df1=pd.DataFrame(pl_list)


    #insert the values of dataframe into sql

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(playlist_id,title,channel_id,channel_name,publishedat,video_count)values(%s,%s,%s,%s,%s,%s)'''
        values=(row['playlist_id'],
               row['title'],
               row['channel_id'],
               row['channel_name'],
               row['publishedat'],
               row['video_count'])
        cur.execute(insert_query,values)
        myconnection.commit()


# In[19]:


def videos_table():
    #connect with mysql

    myconnection = pymysql.connect(host='localhost',user='root',passwd='Soundar@2003',database='youtubedata')
    cur = myconnection.cursor()


    # create table for video details

    drop_query='''drop table if exists videos'''
    cur.execute(drop_query)
    myconnection.commit()


    cur.execute('''create table videos(channel_Name varchar(100),
                channel_Id varchar(100),
                video_id varchar(30) primary key,
                title varchar(150),
                tags text,
                thumbnail varchar(200),
                description text,
                published_date text,
                duration text,
                views bigint,
                likes bigint,
                comments int,
                favoriteCount int,
                definition varchar(10),
                caption_status varchar(50))''')

    #convert mongodb data into dataframe

    vi_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for vi_data in  coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    df2=pd.DataFrame(vi_list)


    #insert the values of dataframe into sql

    for index,row in df2.iterrows():
        insert_query='''insert into videos(channel_Name,
                channel_Id,
                video_id,
                title,
                tags,
                thumbnail,
                description,
                published_date,
                duration,
                views,
                likes,
                comments,
                favoriteCount,
                definition,
                caption_status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
               row[ 'channel_Id'],
               row['video_id'],
               row['title'],
               str(row['tags']),
               row['thumbnail'],
               row['description'],
               row['published_date'],
               row['duration'],
               row['views'],
               row['likes'],
               row['comments'],
               row['favoriteCount'],
               row['definition'],
               row['caption_status'])
        cur.execute(insert_query,values)
        myconnection.commit()


# In[20]:


def comment_table():
    #connect with mysql

    myconnection = pymysql.connect(host='localhost',user='root',passwd='Soundar@2003',database='youtubedata')
    cur = myconnection.cursor()


    # create table for playlist

    drop_query='''drop table if exists comments'''
    cur.execute(drop_query)
    myconnection.commit()

    try:
        cur.execute('''create table comments(comment_id varchar(100) primary key,
                        video_id text,
                        comment_text text,
                        comment_author text,
                        comment_published text)''')
    except:
        print("table already created")

    #convert mongodb data into dataframe

    com_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for com_data in  coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    df3=pd.DataFrame(com_list)
    
    df4=df3.drop_duplicates()

    #insert the values of dataframe into sql


    for index,row in df4.iterrows():
        insert_query='''insert into comments(comment_id,
                        video_id,
                        comment_text,
                        comment_author,
                        comment_published)values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
               row['video_id'],
               row['comment_text'],
               row['comment_author'],
               row['comment_published'])
        cur.execute(insert_query,values)
        myconnection.commit()


# In[21]:


# insert all tables into mysql

def tables():
    Channels_table()
    playlist_table()
    videos_table()
    comment_table()
    
    return "sucessfully tables inserted into mysql"


# In[22]:


#run below function to push all data to mysql from mongo db
#tables()


# In[23]:


#below  4 cells are use to display the tables in streamlit


# In[24]:


def show_channel_table():
    ch_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for ch_data in  coll.find({},{"_id":0,"channel_information":1},):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)
    
    return df


# In[25]:


def show_playlist_table():
    pl_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for pl_data in  coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])

    df1=st.dataframe(pl_list)
    
    return df1


# In[26]:


def show_videos_table():
    vi_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for vi_data in  coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    df2=st.dataframe(vi_list)
    
    return df2


# In[27]:


def show_comment_table():
    com_list=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for com_data in  coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
            
    df3=pd.DataFrame(com_list)        
    df4=df3.drop_duplicates()

    df5=st.dataframe(df4)
    
    
    
    return df5


# In[28]:


#streamlit coding


# In[33]:


with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

    
channel_id=st.text_input("enter the channel id")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll=db["channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_Id"])
        
    if channel_id in ch_ids:
        st.success("given channel id was already exists")
        
    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
if st.button("send the data to mysql from mongodb"):
        table=tables()
        st.success(table)
        
show_table=st.radio("select table for view",("Channels","Playlists","Videos","Comments"))


if show_table == "Channels":
    show_channel_table()
elif show_table == "Playlists":
    show_playlist_table()
elif show_table =="Videos":
    show_videos_table()
elif show_table == "Comments":
    show_comment_table()


# In[49]:


myconnection = pymysql.connect(host='localhost',user='root',passwd='Soundar@2003',database='youtubedata')
cur = myconnection.cursor()

question=st.selectbox("Select any question",("1.What are the names of all the videos and their corresponding channels?",
                                            "2.Which channels have the most number of videos, and how many videos do they have?",
                                            "3.What are the top 10 most viewed videos and their respective channels?",
                                            "4.How many comments were made on each video, and what are their corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8.What are the names of all the channels that have published videos in the year2022?",
                                            "9.What are the top 5 playlists contain most number of videos and their respective channels?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))


# In[54]:


myconnection = pymysql.connect(host='localhost',user='root',passwd='Soundar@2003',database='youtubedata')
cur = myconnection.cursor()

if question=="1.What are the names of all the videos and their corresponding channels?":

    cur.execute('''select title,channel_Name from videos''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["Video Title","Channel Name"]))
    
elif question=='''2.Which channels have the most number of videos, and how many videos do they have?''':
    
    cur.execute('''select channel_Name,total_Videos from channels order by total_Videos desc''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["Channel Name","No.of.Videos"]))
    
elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    
    cur.execute('''select channel_Name,title,views from videos order by views desc limit 10''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["Channel Name","Video Title","No.of.Views"]))
    
elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    
    cur.execute('''select title,comments from videos''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["Video Title","No.of.comment"]))
    
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    
    cur.execute('''select channel_Name,title,likes from videos order by likes desc''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["channel name","video name","No.of.likes"]))
    
elif question=="6.What is the total number of likes for each video, and what are their corresponding video names?":
    
    cur.execute('''select channel_Name,title,likes from videos''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["channel name","video name","No.of.likes"]))
    
elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    
    cur.execute('''select channel_Name,views from channels''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["channel name","No.of.Views"]))
    
elif question=="8.What are the names of all the channels that have published videos in the year2022?":
    
    cur.execute('''select channel_Name,title,published_date from videos where extract(year from published_date)=2022''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["channel name","Video Title","Published Date"]))
    
elif question=="9.What are the top 5 playlists contain most number of videos and their respective channels?":
    
    cur.execute('''select channel_name,title,video_count from playlists order by video_count desc limit 5''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["channel name","Playlist Title","No.of.Videos"]))
    
elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    
    cur.execute('''select channel_Name,title,comments from videos order by comments desc''')
    myconnection.commit()

    st.write(pd.DataFrame(cur.fetchall(),columns=["channel name","Video Title","No.of.Comments"]))


# In[ ]:




