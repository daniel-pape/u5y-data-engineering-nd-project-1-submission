import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: Extracts the song and artist data from filepath
    and saves them to the database.

    Arguments:
        cur: the cursor object used for querying the database
        filepath: song data file path.

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines='True')

    # insert song record
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = list(df[columns].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = list(df[columns].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: Extracts the time, user and songplay data from filepath
    and saves them to the database.

    Arguments:
        cur: the cursor object used for querying the database
        filepath: log data file path.

    Returns:
        None
    """
    # open log file
    df=pd.read_json(filepath, lines='True')

    # filter by NextSong action
    filter = (df['page'] == 'NextSong')
    df = df[filter]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year']
    data = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    given_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    columns_mapping = dict(zip(given_columns, columns))
    user_df = df[given_columns].rename(columns_mapping)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, round(row.length,3)))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(conn, filepath, func):
    """
    Description: Processes all JSON files found in the directory
    specified by filepath by extracting and saving the required
    data for the songs, artists, users, time and songplays tables.

    Arguments:
        conn: the connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    cur=conn.cursor()

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

    process_data(conn, filepath='data/song_data', func=process_song_file)
    process_data(conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()