import boto3
import json
import tables
import tempfile
import h5py
from hd5_getters import *


with open('config.json', 'r') as f:
    config = json.load(f)

s3=boto3.client('s3')

def process_h5_file(filename):
    # h5tocopy = open_h5_file_read(filename)
    # song_num = get_num_songs(h5tocopy)
    # song_list=[]
    # for songidx in range(song_num):
    #     song_info = []
    #     # METADATA
    #     song_info.append(float(get_artist_familiarity(h5tocopy,songidx)))
    #     song_info.append(float(get_artist_hotttnesss(h5tocopy,songidx)))
    #     song_info.append(str(get_artist_id(h5tocopy,songidx)))
    #     song_info.append(str(get_artist_location(h5tocopy,songidx)))
    #     song_info.append(get_artist_mbtags(h5tocopy,songidx).tolist())
    #     song_info.append(get_artist_mbtags_count(h5tocopy,songidx).tolist())
    #     song_info.append(str(get_artist_name(h5tocopy,songidx)))
    #     song_info.append(get_artist_terms(h5tocopy,songidx).tolist())
    #     song_info.append(get_artist_terms_freq(h5tocopy,songidx).tolist())
    #     song_info.append(get_artist_terms_weight(h5tocopy,songidx).tolist())
    #     song_info.append(float(get_danceability(h5tocopy,songidx)))
    #     song_info.append(float(get_duration(h5tocopy,songidx)))
    #     song_info.append(float(get_end_of_fade_in(h5tocopy,songidx)))
    #     song_info.append(float(get_energy(h5tocopy,songidx)))
    #     song_info.append(float(get_key(h5tocopy,songidx)))
    #     song_info.append(float(get_key_confidence(h5tocopy,songidx)))
    #     song_info.append(float(get_loudness(h5tocopy,songidx)))
    #     song_info.append(float(get_mode(h5tocopy,songidx)))
    #     song_info.append(float(get_mode_confidence(h5tocopy,songidx)))
    #     song_info.append(str(get_release(h5tocopy,songidx)))
    #     song_info.append(get_segments_confidence(h5tocopy,songidx).tolist())        
    #     song_info.append(get_segments_loudness_max(h5tocopy,songidx).tolist())        
    #     song_info.append(get_segments_loudness_max_time(h5tocopy,songidx).tolist())    
    #     song_info.append(get_segments_pitches(h5tocopy,songidx).tolist())    
    #     song_info.append(get_segments_timbre(h5tocopy,songidx).tolist())    
    #     song_info.append(get_similar_artists(h5tocopy,songidx).tolist())   
    #     song_info.append(float(get_artist_hotttnesss(h5tocopy,songidx)))
    #     song_info.append(str(get_song_id(h5tocopy,songidx)))
    #     song_info.append(float(get_start_of_fade_out(h5tocopy,songidx)))
    #     song_info.append(float(get_tempo(h5tocopy,songidx)))
    #     song_info.append(int(get_time_signature(h5tocopy,songidx)))
    #     song_info.append(float(get_time_signature_confidence(h5tocopy,songidx)))
    #     song_info.append(str(get_title(h5tocopy,songidx)))
    #     song_info.append(str(get_track_id(h5tocopy,songidx)))
    #     song_info.append(int(get_year(h5tocopy,songidx)))
    #     song_list.append(song_info)
    
    # h5tocopy.close()
    return filename['metadata']['songs'][:1]['artist_familiarity'][0]
   


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
            with h5py.File(tmp.name) as h5:
                return process_h5_file(h5)
        except Exception as e:
            print(str(e))
            return []

def main():
    bucket = "songsbuckettest"
    prefix  = "data/A/R/A/"
    processed=[]
    for prefix in get_prefixes(bucket,prefix):
        processed.append(transform_s3(prefix))

    print(processed[0])

if __name__ =="__main__":
    main()