import streamlit as st
from PIL import Image
import googleapiclient.discovery
import mysql.connector
import googleapiclient.errors
import sys
from PIL import Image
import pandas as pd


# ********************************************************HEADER CREATION************************************************************************
# Fetch image from URL
#************************************************************************************************************************************* 
# File path to the image
file_path = r"C:\Users\premi\Desktop\Premila\projects\project1data\guvi pic(1).jpg"

# Open the image
image = Image.open(file_path)

# Display the image and Title
st.image(image, caption='', width=300)

# Streamlit UI
st.title('**GUVI PROJECT**')
st.subheader('**DATA HARVESTING**')
st.write("***********************************************************************************************************************")
st.balloons()

# OPTION 1**********************************************************************************************************************************

# Function to display videos based on channel selection
def display_video(channel):
    video_urls = {
        'Channel 1': 'https://www.youtube.com/watch?v=J38Yq85ZoyY',
        'Channel 2': 'https://www.youtube.com/watch?v=yu_x8qv6mfA',
        'Channel 3': 'https://www.youtube.com/watch?v=B0MUXtmSpiA',
        'Channel 4': 'https://www.youtube.com/watch?v=EDap9qxb96k',
        'Channel 5': 'https://www.youtube.com/watch?v=mGYDwB3RaQs',
        'Channel 6': 'https://www.youtube.com/watch?v=jNQXAC9IVRw',
        'Channel 7': 'https://www.youtube.com/watch?v=Y9i3OIMitRQ',
        'Channel 8': 'https://www.youtube.com/watch?v=-yulNRjtLAc',
        'Channel 9': 'https://www.youtube.com/watch?v=ri-BM1i4WSs',
        'Channel 10': 'https://www.youtube.com/watch?v=71h8MZshGSs'
    }
    if channel in video_urls:
        st.video(video_urls[channel])
    else:
        st.error("Video not found")

# OPTION 2************************************************************************************************* 

# Initialize YouTube API
api_key = "AIzaSyDw55ceYW43trQvQYrcolmFjNFoONcOeCg"  
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

# st.title("DATA HARVESTING - YOUTUBE API")

# Function to fetch channel details

def fetch_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,status",
        id=channel_id
    )
    response = request.execute()

    data = {
        'channel_id': response['items'][0]['id'],
        'channel_name': response['items'][0]['snippet']['title'],
        'channel_type': response['items'][0]['snippet']['localized']['title'],
        'view_count': response['items'][0]['statistics']['viewCount'],
        'channel_description': response['items'][0]['snippet']['description'],
        'channel_status': response['items'][0]['status']['privacyStatus'],
        'channel_video_id' : response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return data

# Function to fetch playlist details

def fetch_playlist_details(channel_id):
    
        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50
        )
        response = request.execute()

        playlist_details = []
        for playlist in response.get('items', []):
            data = {
                'playlist_id': playlist['id'],
                'channel_id': playlist['snippet']['channelId'],
                'playlist_name': playlist['snippet']['title'],
                'channel_name': playlist['snippet']['channelTitle']
            }
            playlist_details.append(data)

        return playlist_details

# # Function to fetch video details

def fetch_video_details(channel_id):
    video_details = []
    next_page_token = None

    try:
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            type="video",
            maxResults=100,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get('items', []):
            if item['id']['kind'] == 'youtube#video':
                video_id = item['id']['videoId']
                video_request = youtube.videos().list(
                    part="snippet,statistics,status,contentDetails",
                    id=video_id
                )
                video_response = video_request.execute()
                if 'items' in video_response:
                    video = video_response['items'][0]
                    data = {
                        'video_id': video['id'],
                        'channel_id': video['snippet']['channelId'],
                        'video_name': video['snippet']['title'],
                        'video_description': video['snippet']['description'],
                        'published_date': video['snippet']['publishedAt'][:-1],
                        'view_count': video['statistics']['viewCount'],
                        'favorite_count': video['statistics'].get('favoriteCount', 0),
                        'dislike_count': video['statistics'].get('dislikeCount', 0),
                        'comment_count': video['statistics'].get('commentCount', 0),
                        'duration': video['contentDetails']['duration'],
                        'thumbnail': video['snippet']['thumbnails']['default']['url'],
                        'caption_status': video['status']['uploadStatus']
                    }
                    video_details.append(data)
    except Exception as e:
        st.error(f"Error fetching videos: {str(e)}")

    return video_details

# Function to Fetch comment details

def fetch_comment_details(video_ids):
        comment_details = []
    
        for video_id in video_ids:
            try:
                next_page_token = None
            
                while True:
                    request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                )
                    response = request.execute()

                    for item in response.get('items', []):
                         comment = item['snippet']['topLevelComment']
                    data = {
                        'comment_id': comment['id'],
                        'video_id': item['snippet']['videoId'],
                        'comment': comment['snippet']['textDisplay'],
                        'comment_published_date': comment['snippet']['publishedAt'][:-1],
                        'comment_author': comment['snippet']['authorDisplayName']
                    }
                    comment_details.append(data)

                    next_page_token = response.get('nextPageToken')

                    if not next_page_token:
                        break
            except Exception as e:
                    print(f"Error fetching comments for video {video_id}: {str(e)}")

        return comment_details


# OPTION 3*****************************************************EXPORT YOUTUBE AP DATA TO MYSQL TABLES*****************************************
# **************************************************************CREATING TABLES*******************************************************

api_key = "AIzaSyDw55ceYW43trQvQYrcolmFjNFoONcOeCg"  
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

conn = mysql.connector.connect(
        host ='localhost',
        user =  'root',
        password= 'Jesus@2525',
        database='youtube'
        )
cursor = conn.cursor()

def create_tables_data():

# Creating the 'channel' table

    sql1 = '''CREATE TABLE IF NOT EXISTS channel (
                channel_id VARCHAR(255),
                channel_name VARCHAR(255),
                channel_type VARCHAR(255),
                channel_views INT,
                channel_description TEXT,
                channel_status VARCHAR(255),
                Primary Key(channel_id)
            )'''
    cursor.execute(sql1)

# Creating the 'playlist' table

    sql2 = '''CREATE TABLE IF NOT EXISTS playlist (
                playlist_id VARCHAR(255),
                channel_id VARCHAR(255),
                playlist_name VARCHAR(255),
                channel_name VARCHAR(255),
                primary key(playlist_id),
                foreign key(channel_id) references channel(channel_id)
            )'''
    cursor.execute(sql2)

# Creating the 'comment' table

    sql3 = '''CREATE TABLE IF NOT EXISTS comment (
        comment_id VARCHAR(255),
        video_id VARCHAR(255),
        comment TEXT,
        comment_published_date DATETIME,
        comment_author TEXT,
        PRIMARY KEY (comment_id),
        FOREIGN KEY (video_id) REFERENCES video(video_id)
        )'''
    cursor.execute(sql3)

# Creating the 'video' table

    sql4 = '''CREATE TABLE IF NOT EXISTS video (
                video_id VARCHAR(255),
                channel_id VARCHAR(255),
                video_name VARCHAR(255),
                video_description TEXT,
                published_date DATETIME,
                view_count INT,
                dislike_count INT,
                favourite_count INT,
                comment_count INT,
                duration VARCHAR(255),
                Thumbnail VARCHAR(255),
                caption_status VARCHAR(255),
                primary key(video_id),
                foreign key(playlist_id) references playlist(playlist_id)
            )'''
    cursor.execute(sql4)

# *********************************************************************************************************************
# Function to insert channel data into MySQL table

conn = mysql.connector.connect(
        host ='localhost',
        user =  'root',
        password= 'Jesus@2525',
        database='youtube'
        )
cursor = conn.cursor()

def insert_channel_data(data):
      
    Isql1 = "INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (
        data['channel_id'],
        data['channel_name'],
        data['channel_type'],
        data['view_count'],
        data['channel_description'],
        data['channel_status']
    )
    try:
        cursor.execute(Isql1, val)  
        conn.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()  # Rollback in case of error

# Function to insert playlist data into MySQL table

def insert_playlist_data(data):
    Isql2 = '''
        INSERT INTO playlist (playlist_id, channel_id, playlist_name, channel_name)
        VALUES (%s, %s, %s, %s)
    '''
    val = (
        data['playlist_id'],
        data['channel_id'],
        data['playlist_name'],
        data['channel_name']
    )
    try:
        cursor.execute(Isql2, val)
        conn.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as e:
        print(f"Error inserting data: {e}")
        # conn.rollback()  # Rollback in case of error

# Function to insert comment data into MySQL table

def insert_comment_data(data):
    
    Isql3 = '''
        INSERT INTO comment (comment_id, video_id, comment, comment_published_date,comment_author)
        VALUES (%s, %s, %s, %s, %s)
    '''
    val = (
        data['comment_id'],
        data['video_id'],
        data['comment'],
        data['comment_published_date'],
        data['comment_author']
          )
    try:
        cursor.execute(Isql3, val)
        conn.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as e:
        print(f"Error inserting data: {e}")
        # conn.rollback()  # Rollback in case of error

# Function to insert video data into MySQL table

def insert_video_data(data):
    Isql4 = '''
        INSERT INTO video (video_id, channel_id, video_name, video_description, published_date,
                           view_count, dislike_count, favourite_count, comment_count, duration, thumbnail, caption_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    val = (
        data['video_id'],
        data['channel_id'],
        data['video_name'],
        data['video_description'],
        data['published_date'],
        data['view_count'],
        data['dislike_count'],
        data['favorite_count'],
        data['comment_count'],
        data['duration'],
        data['thumbnail'],
        data['caption_status']
    )
    try:
        cursor.execute(Isql4, val)
        conn.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as e:
        print(f"Error inserting data: {e}")
        # conn.rollback()  # Rollback in case of error
        # conn.close


# **************************************************************************************************************

# # Committing the changes and closing the connection

    conn.commit()
    # conn.close()

# ***************************************************************************************************************
# Streamlit UI

def main():
     
    option = st.sidebar.selectbox('What would you like to do?',
                                  ['Watch a YouTube video', 'Fetch Data from YOUTUBE API?',
                                   'EXPORT API data to MYSQL Tables', 'Query THE mysql TABLES', 'Exit'])

# 1. OPTION ***********************************************SEEING YOUTUBE CHANNEL*****************

    if option == 'Watch a YouTube video':
        selected_channel = st.selectbox("Please select the YouTube channel to watch:",
                                        ['Channel 1', 'Channel 2', 'Channel 3', 'Channel 4', 'Channel 5',
                                         'Channel 6', 'Channel 7', 'Channel 8', 'Channel 9', 'Channel 10'])
        display_video(selected_channel)

# 2. OPTION ***********************************************FETCHING DATA FROM YOUTUBE API*****************
    elif option == 'Fetch Data from YOUTUBE API?':
        # Streamlit UI
        ch_id = st.text_input("Enter Channel ID:")
        if ch_id and st.button("Retrieve Channel Data"):
            channel_data = fetch_channel_details(ch_id)
            st.write(channel_data)

            playlists_data = fetch_playlist_details(ch_id)
            if playlists_data:
                st.title("Playlists Details")
                for playlist_data in playlists_data:
                    st.write(playlist_data)

            comments_data = fetch_comment_details(ch_id)
            if comments_data:
                st.title("Comments Details")
                for comment_data in comments_data:
                    st.write(comment_data)

            videos_data = fetch_video_details(ch_id)
            if videos_data:
                st.title("Video Details")
                for video_data in videos_data:
                    st.write(video_data)

# 3. OPTION*******************************INSERT DATA TO MYSQL TABLES*****************************************************

# EXPORT API data to MYSQL Tables

    elif option == 'EXPORT API data to MYSQL Tables':
                
        ch_id = st.text_input("Enter Channel ID:")
        if st.button ('Export API Data to MYSQL'):
                    create_tables_data()
                    if ch_id:
# Fetch and insert channel data

                        channel_data = fetch_channel_details(ch_id)
                        if channel_data:
                            insert_channel_data(channel_data)
                            st.success('Channel data inserted successfully into CHANNEL MySQL table.')    

                        else:
                            st.error("No channel found with the provided ID.")
                      # st.success('Channel data inserted successfully into CHANNEL MySQL table.')    

# Fetch and inserting data to playlist table

                        playlists_data = fetch_playlist_details(ch_id)
                        for playlist_data in playlists_data:
                            if playlist_data:
                                insert_playlist_data(playlist_data)
                            else:
                                st.error("No Playlist found with the provided ID.")
                        st.success('Playlist data inserted successfully into the "PLAYLIST" MySQL table.')

# Fetch and insert video data

                        videos_data = fetch_video_details(ch_id)
                        for video_data in videos_data:
                            if video_data:
                                insert_video_data(video_data)
                            else:
                                st.error("No videos found for the channel.")
                        st.success("Video data inserted successfully into  VIDEO  MySQL tables.")
               
# Fetch and insert comment data

                        comments_data = fetch_comment_details([video['video_id'] for video in fetch_video_details(ch_id)])
                        for comment_data in comments_data:
                            if comment_data:
                                insert_comment_data(comment_data)
                            else:
                                st.error("No comments found with the provided ID.")
                        st.success('Comment data inserted successfully into the COMMENT MySQL table.')        
 


# 4.option ****************************QUERYING THE MYSQL TABLES********************

    elif option == 'Query THE mysql TABLES':
                                         
                                      
# Establish MySQL connection
        conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Jesus@2525",
        database="youtube"  
         )

# Creating a cursor object
        cursor = conn.cursor()

# Define Streamlit columns
        col1, col2 = st.columns([4, 10])

# Column 1: Input SQL Query
        with col1:
            st.text("ENTER YOUR SQL QUERY:")
            query_input = st.text_area("SQL Query", height=200)

# Column 3: Execute and Display Results
        with col2:
            if st.button("Execute"):
                st.text("Display Query Results:")
                cursor.execute(query_input)
                query_results = cursor.fetchall()  # Fetch all the results
                if query_results:
            # Convert query_results to DataFrame
                    df = pd.DataFrame(query_results, columns=[i[0] for i in cursor.description])
                    st.write(df)
            else:
                st.write("No results found.")    
    
        st.write('''
                        1.What are the names of all the videos and their corresponding channel 
            
                        Ans :-

                        SELECT v.video_name, c.channel_name
                        FROM video v
                        JOIN channel c ON v.channel_id = c.channel_id;

                        Explanation :
                        This query selects the video name from the video table and the channel name from the channel table, 
                        joining them on the channel_id column to ensure that each video is associated with its corresponding channel. 
                        This will give you the names of all videos along with their corresponding channel names.

                        2.Which channels have the most no of videos and how many videos do they have.
                       
                        Ans:-
                       
                        SELECT c.channel_id, c.channel_name, COUNT(v.video_id) AS num_videos
                        FROM channel c
                        JOIN video v ON c.channel_id = v.channel_id
                        GROUP BY c.channel_id, c.channel_name
                        ORDER BY num_videos DESC;

                        EXPLAINATION: 
                        This query retrieves the channel ID and name from the channel table, counts the number of videos associated 
                        with each channel using the video table, and groups the results by channel. 
                        It then orders the result set by the number of videos in descending order, so you'll see the channels with the most videos at the top.

                        3.What are the top 10 most viewed videos and their respective channels
                       
                        Ans :

                        SELECT v.video_name, c.channel_name, v.view_count
                        FROM video v
                        JOIN channel c ON v.channel_id = c.channel_id
                        ORDER BY v.view_count DESC
                        LIMIT 10;

                        EXPLAINATION: 
                        This query selects the video name from the video table, the channel name from the channel table, 
                        and the view count from the video table. It joins the two tables on the channel_id column to ensure 
                        that each video is associated with its corresponding channel. 
                        The results are then ordered by view count in descending order and limited to the top 10 rows, 
                        giving you the top 10 most viewed videos and their respective channels.

                        4. How many comments are made on each video and their corresponding video names.

                        Ans :
                       
                        SELECT v.video_name, COUNT(c.comment_id) AS num_comments
                        FROM video v
                        JOIN comment c ON v.video_id = c.video_id
                        GROUP BY v.video_name;

                        EXPLAINATION: 
                        This query selects the video name from the video table and counts the number of comments associated with 
                        each video using the comment table. It joins the two tables on the video_id column to ensure that each comment 
                        is associated with its corresponding video. The results are then grouped by video name, giving the number of 
                        comments made on each video along with their corresponding video names.

                        5.Which videos have the highest number of likes and what are their corresponding vide names.

                        Ans :
                       
                        SELECT v.video_name, v.like_count
                        FROM video v
                        ORDER BY v.like_count DESC
                        LIMIT 10;

                        EXPLAINATION: 
                        This query selects the video name and like count from the video table. 
                        It then orders the results by like count in descending order and limits the output to the top 10 rows. 
                        This will give the videos with the highest number of likes and their corresponding names.

                        6.What is the total number of likes and dislikes for each video , and  what are their corresponding video names

                        Ans :
                       
                        SELECT 
                            v.video_name, 
                            SUM(v.like_count) AS total_likes, 
                            SUM(v.dislike_count) AS total_dislikes
                        FROM 
                            video v
                        GROUP BY 
                            v.video_name;

                        EXPLAINATION: 

                        This query selects the video name from the video table and sums up the like_count and dislike_count for each video. 
                        It then groups the results by video name. 
                        This will give you the total number of likes and dislikes for each video along with their corresponding video names.


                        7. What are the total number of views for each channel, and what are their corresponding channel names.

                        Ans:
                       
                        SELECT c.channel_name, SUM(v.view_count) AS total_views
                        FROM channel c
                        JOIN video v ON c.channel_id = v.channel_id
                        GROUP BY c.channel_name;

                        EXPLAINATION: 
                        This query selects the channel name from the channel table and sums up the view_count for each channel's videos 
                        using the video table. It then groups the results by channel name. This will give you the total number of views 
                        for each channel along with their corresponding channel names.


                        8.What are the names of all the channels that have published videos in the year 2022

                        Ans :

                        SELECT DISTINCT c.channel_name
                        FROM channel c
                        JOIN video v ON c.channel_id = v.channel_id
                        WHERE YEAR(v.published_date) = 2022;

                        EXPLAINATION: 

                        This query selects distinct channel names from the channel table that have published videos in the year 2022. 
                        It joins the channel and video tables on the channel_id column, filters the videos based on the published_date 
                        falling within the year 2022, and then retrieves the distinct channel names.

                        9. What is the average duration of all the videos in each channel, and what are their corresponding channel names.

                        Ans :

                       SELECT 
    c.channel_name, 
    AVG(
        TIME_TO_SEC(
            CASE
                WHEN LOCATE('H', v.duration) > 0 THEN
                    SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'H', 1), 'T', -1) * 3600 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'M', 1), 'H', -1) * 60 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'S', 1), 'M', -1)
                WHEN LOCATE('M', v.duration) > 0 THEN
                    SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'M', 1), 'T', -1) * 60 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'S', 1), 'M', -1)
                ELSE
                    SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'S', 1), 'T', -1)
            END
        )
    ) AS avg_duration_seconds
FROM 
    channel c
JOIN 
    video v ON c.channel_id = v.channel_id
GROUP BY 
    c.channel_name;

EXPLAINATION: 
We're using LOCATE function to check if 'H' (hours) or 'M' (minutes) are present in the duration string.
Based on their presence, we extract the corresponding hour, minute, and second parts and convert them into seconds.
The CASE statement handles different scenarios depending on whether hours, minutes, or only seconds are present in the duration string.
Finally, the average duration in seconds is calculated for each channel.
This query should give you the average duration of videos in seconds for each channel, considering the provided datatype  format fo and datafetched for duration is varchar(255). 


                        10.Which videos have the highest number of comments and what are their
                        Corresponding channel names.

                        Ans :

                        SELECT v.video_name, c.channel_name, COUNT(co.comment_id) AS num_comments
                        FROM video v
                        JOIN channel c ON v.channel_id = c.channel_id
                        JOIN comment co ON v.video_id = co.video_id
                        GROUP BY v.video_name, c.channel_name
                        ORDER BY num_comments DESC
                        LIMIT 10;

                        EXPLAINATION: 

                        This query selects the video name from the video table, the channel name from the channel table, and counts the number 
                        of comments associated with each video using the comment table. It then joins the three tables together on their respective 
                        IDs and groups the results by video name and channel name. Finally, it orders the results by the number of comments in 
                        descending order and limits the output to the top 10 rows.
                        This will give you the videos with the highest number of comments and their corresponding channel names.
                        ''')
# 5. option********************************EXIT FROM THE PROJECT************************************

    elif option == 'Exit':
        st.write('Goodbye!')
        sys.exit()
       


if __name__ == '__main__':
            main()



