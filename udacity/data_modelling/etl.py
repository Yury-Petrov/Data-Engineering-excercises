import glob
import pandas as pd
import psycopg2

from sql_queries import *


########################################################################################################################
#                                                                                                                      #
#                                   functions based on the data/song_data' files                                       #
#                                                                                                                      #
########################################################################################################################


def process_songs(cur, df):
    # Transform the data and prepare for insertion
    song_column_name_mapping = {
        'song_id': 'songId',
        'title': 'title',
        'artist_id': 'artistId',
        'year': 'year',
        'duration': 'duration'
    }
    song_df = df.drop_duplicates('song_id').filter(items=list(song_column_name_mapping.keys())).rename(
        columns=song_column_name_mapping
    )

    # Load the data into database
    for i, song_row in song_df.iterrows():
        cur.execute(song_table_insert, song_row.to_dict())


def process_artists(cur, df):
    process_songs(cur=cur, df=df)
    artist_column_name_mapping = {
        'artist_id': 'artistId',
        'artist_name': 'name',
        'artist_location': 'location',
        'artist_latitude': 'latitude',
        'artist_longitude': 'longitude'
    }
    artist_df = df.filter(items=list(artist_column_name_mapping.keys())).rename(
        columns=artist_column_name_mapping)
    for i, artist_row in artist_df.iterrows():
        cur.execute(artist_table_insert, artist_row.to_dict())


########################################################################################################################
#                                                                                                                      #
#                                   functions based on the data/log_data' files                                        #
#                                                                                                                      #
########################################################################################################################

def process_time(cur, df):
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # prepare time
    time_df = df.filter(items=['ts']) \
        .drop_duplicates(subset=['ts']) \
        .rename(columns={'ts': 'startTime'}) \
        .assign(ts=lambda r: pd.to_datetime(r.startTime, unit='ms')) \
        .assign(year=lambda r: r.ts.dt.year) \
        .assign(month=lambda r: r.ts.dt.month) \
        .assign(week=lambda r: r.ts.dt.week) \
        .assign(weekday=lambda r: r.ts.dt.weekday) \
        .assign(day=lambda r: r.ts.dt.day) \
        .assign(hour=lambda r: r.ts.dt.hour) \
        .drop(columns=['ts'])

    for i, time_row in time_df.iterrows():
        cur.execute(time_table_insert, time_row.to_dict())


def process_users(cur, df):
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    # load user table
    user_column_names = ['userId', 'firstName', 'lastName', 'gender', 'level']

    user_df = df.dropna(subset=['userId']).assign(userId=lambda r: r.userId.astype(str)).filter(
        items=user_column_names)
    #
    # # insert user records
    for i, user_row in user_df.iterrows():
        cur.execute(user_table_insert, user_row)


def process_songplays(cur, df):
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    # # insert songplay records
    for i, log_row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, {
            'title': log_row.song,
            'artistName': log_row.artist,
            'duration': log_row.length
        })
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = {
            'startTime': log_row.ts,
            'userId': log_row.userId,
            'level': log_row.level,
            'songId': songid,
            'artistId': artistid,
            'sessionId': log_row.sessionId,
            'location': log_row.location,
            'userAgent': log_row.userAgent
        }
        cur.execute(songplay_table_insert, songplay_data)

########################################################################################################################
#                                                                                                                      #
#                                               helper functions                                                       #
#                                                                                                                      #
########################################################################################################################


def get_files_as_dataframe(path_to_files):
    """
    Recursively goes through a group files in a given directory and generates a single dataframe for those files.
    :param path_to_files: path to files that need to be extracted.
    :return: dataframe with the data of all the files in the file group.
    """
    all_files = glob.glob(path_to_files, recursive=True)
    print('{} files found in {}'.format(len(all_files), path_to_files))

    return pd.concat([pd.read_json(file_name, lines=True) for file_name in all_files])


def process_data(cur, conn, filepath, funcs):
    """
    The function extracts the files in a given path to a dataframe and then transforms and saves it by invoking the
    func functions passed to it as a parameter. Only works with json files at the moment. Any non-json files will be
    ignored.
    :param cur: cursor to execute data load against
    :param conn: connection to the database
    :param filepath: upper-level path to a file group
    :param funcs: functions to with the logic to save the data to a database.
    If an additional table needs to be populated from the same file group, just add a specific to that table function to
    the funcs array
    """
    filepath_recursive_postfix = '**/*.json'
    # Get the dataframe from all the files
    df = get_files_as_dataframe(f'{filepath}/{filepath_recursive_postfix}')
    for func in funcs:
        func(cur, df)
    conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    song_data_file_path = 'data/song_data'
    print(f"Processing file path:{song_data_file_path}")
    process_data(cur, conn, filepath=song_data_file_path, funcs=[process_songs, process_artists])
    log_data_file_path = 'data/log_data'

    print(f"Processing file path:{log_data_file_path}")
    process_data(cur, conn, filepath=log_data_file_path, funcs=[process_time, process_users, process_songplays])
    conn.close()

    print("ETL job complete")


if __name__ == "__main__":
    main()
