import boto3
import json
import tables
import tempfile
import h5py
import os
import traceback
import pandas as pd 
from tqdm import tqdm
from hd5_getters import *


s3=boto3.client('s3')

def process_h5_file(filename):
    h5File = open_h5_file_read(filename)
    song_num = get_num_songs(h5File)    
    result = []
    for songidx in range(song_num):
        song_info = []
        song_info.append(float(get_artist_familiarity(h5File,songidx)))
        song_info.append(float(get_duration(h5File,songidx)))
        song_info.append(float(get_key(h5File,songidx)))
        song_info.append(float(get_loudness(h5File,songidx)))
        song_info.append(float(get_mode(h5File,songidx)))
        song_info.append(float(get_artist_hotttnesss(h5File,songidx)))# Song hotness
        song_info.append(float(get_tempo(h5tocopy,songidx)))
        song_info.append(float(get_time_signature_confidence(h5File,songidx)))
        song_info.append(str(get_title(h5File,songidx)))
        song_info.append(str(get_track_id(h5File,songidx)))
        song_info.append(int(get_year(h5File,songidx)))

        result.append(song_info)
    h5File.close()
    return result[0]   

    # return filename['metadata']['songs'][:1]['artist_familiarity']
   

def get_prefixes(bucket, prefix):
    # In order to run with multiple threads/machines at a time, the prefix could be set to different things,
    # to make sure there is no overlap. For example, 'million-song/data/A', 'million-song/data/B', ...
    
    s3 = boto3.client('s3')

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    prefixes = [content['Key'] for content in response['Contents']]

    while response['IsTruncated']:
        response = s3.list_objects_v2(Bucket=bucket, 
                                      Prefix=prefix, 
                                      ContinuationToken=response['NextContinuationToken'])
        prefixes.extend([content['Key'] for content in response['Contents']])
        
    return prefixes

def transform_s3(key, bucket="songsbuckettest"):
    """
    REMEBER TO DO DEFENSIVE PROGRAMMING, WRAP IN TRY/CATCH
    """
    s3 = boto3.client('s3')
    # print("connection to s3 -- Test")
    with tempfile.NamedTemporaryFile(mode='wb') as tmp:
        s3.download_fileobj(bucket, key, tmp)
    
        try:
            return process_h5_file(tmp.name)
        except Exception as e:
            return []


def load_config():
    with open('config.json', 'r') as f:
        return(json.load(f))

def rows_to_file(rows,count,path,bucket):
    print(len(rows))
    if(len(rows)==0):
        return

    col_name = ["artist_familiarity", "duration", "key", "loudness", "mode", 
"song_hotttnesss", "song_id", "tempo", 
"time_signature_confidence", "title", "track_id", "year"]
    song_df = pd.DataFrame(columns=col_name)
    try:
        for row in rows:
            if(len(row) !=0):
                row_series =pd.Series(row,index=col_name)
                song_df=song_df.append(row_series,ignore_index=True)

        temp_path = f"/Users/rohanbansal/Documents/CMU/Sem_2/10605/project/MillionSongSubset/Million_Song_Analysis/s3access/temp_{count}.csv"
        song_df.to_csv(temp_path)
        name = path + f"outsongs_{count}.csv"
        
        response = s3.upload_file(temp_path, bucket, name)
        os.remove(temp_path)
    except Exception as e:
            return
    
    return
    
    # Write some code to save a list of rows into a temporary CSV
    # for example using pandas.



def main():
    # bucket = "songsbuckettest"
    conf =  load_config()
    bucket =  conf["bucket_name"]
    # prefix  = "data/A/"
    prefix  = conf["load_path"]
    final_path = "processed/rohan/"
    # final_path= conf["out_path"]
    processed=[]
    chunk_size = conf["chuncksize"]
    out_file_count =0
    for prefix in tqdm(get_prefixes(bucket,prefix)):
        processed.append(transform_s3(prefix))

        if len(processed) % chunk_size == 0:
            out_file_count = out_file_count+1
            rows_to_file(processed,out_file_count,final_path,bucket) 
        # # upload to s3. make sure to not overwrite the name
            processed = []

    # print(len(processed[1]))

if __name__ =="__main__":
    main()