import os
import glob
import psycopg2
import pandas as pd
from sql_queries import song_table_insert, artist_table_insert, time_table_insert,\
user_table_insert, song_select, songplay_table_insert
import datetime

def process_song_file(cur, filepath):
    """
    Perform ETL on song_data file.
    Create songs and artists dimensional tables.
    Args:
        cur: cursor(psycopg2.cursor) - The psycopg2 cursor
        filepath: Single song file path, extract details and insert into song and artist table.
    Output:
        None
    """
    
    # Open song_data file
    df = pd.read_json(filepath, lines=True)

    # Perform ETL on song_data file

    # Extract values from song_data to build songs table
    
    # Build tupple with values from song_data for songs table
    song_data = df[['song_id', 'title','artist_id', 'year', 'duration']].values[0].tolist()
    song_data = (song_data[0], song_data[1], song_data[2], song_data[3], song_data[4])
    # Insert song values into the songs table
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e: 
        print("Error: Could not insert song values of row ")
        print(e)
        pass
    
    # Extract values from song_data to build artists table
    
    # Build tupple with values from song_data for artists table
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    artist_data = (artist_data[0], artist_data[1], artist_data[2], artist_data[3], artist_data[4])
    # Insert artist values into the artists table
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e: 
        print("Error: Could not insert artist values of row ")
        print(e)
        pass


def process_log_file(cur, filepath):
    '''
    Perform ETL on log_data file.
    Create time and users dimensional tables, and songplays fact table.
    Args:
        cur: cursor(psycopg2.cursor) - The psycopg2 cursor
        filepath: Single log file path, extract details and insert into time and users dimensional tables, and songplays fact table.
    Output:
        None
    '''
    
    # Open log_data file
    df = pd.read_json(filepath, lines=True)

    # Perform ETL on log_data file
    
    # Filter records by NextSong action
    df =  df[df['page']=="NextSong"].reset_index()
   
    # Extract values from log_data to build time table
    
    # Convert timestamp (in log data the timestamp is originally in millisecond) "ts" column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    # Create column "week" for the time table
    df['week'] = t.apply(lambda x: datetime.date(x.year, x.month, x.day).isocalendar()[1])
    # Create column "week_day" for the time table
    df['week_day'] = t.apply(lambda x: datetime.date(x.year, x.month, x.day).strftime("%A"))
    # Create column "start_time" for the time table
    df['start_time'] = t
    # Build time_df table to build the time data
    time_data = (t, t.dt.hour, t.dt.day, df.week, t.dt.month, t.dt.year, df.week_day)
    time_column_labels = ['start_time','hour','day','week','month', 'year','weekday']
    time_df = pd.DataFrame(dict(zip(time_column_labels, time_data)))
    # For each rows in the time_df table
    for i, row in time_df.iterrows():
        # Insert values into the time table
        cur.execute(time_table_insert, list(row))

    # Extract values from log_data to build users table

    # Build user_df table to build the users data
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]
    # For each rows in the user_df table
    for i, row in user_df.iterrows():
        # Insert values into the users table
        cur.execute(user_table_insert, row)
        
    # Extract values from log_data to build songplays table

    # For each rows in the df table
    for index, row in df.iterrows():
        # Get songid and artistid from songs and artists tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        # Store songid and artistid in results variable
        results = cur.fetchone()
        
        # If results exist then store songid in song_id and artistid in artist_id
        if results:
            songid, artistid = results
        # Else set up artist_id and song_id as empty
        else:
            #print("Error: artistid and songid not found")
            continue

        # Insert values into songplays table
        songplay_data = (row.start_time,row.userId,row.level,songid,artistid, row.sessionId,row.location,row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e: 
            print("Error: Could not insert songplays values of row ")
            print(e)
            pass


def process_data(cur, conn, filepath, func):
    '''
    Perform ETL process function on files in the filepath.
    Args:
        cur: cursor(psycopg2.cursor) - The psycopg2 cursor
        connection(psycopg2.connection): The sparkifydb connection
        filepath: Dataset path to extract all the sub paths
        func: ETL function to call and process the data
    Output:
        None
    '''
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Connect to Postgresql, create new DB (sparkifydb),
    Get all files (song and log data).
    Perform ETL on song data and log data.
    Insert records into dimensions table and fact table.
    Args: 
        None
    Output:
        None
    """
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=140139") #connect to the sparkify database
    cur = conn.cursor() 

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    #connection closed
    conn.close()


if __name__ == "__main__":
    main()