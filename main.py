from hdfs import InsecureClient
import pandas as pd
import sys


def process_files(log_file):
    file_name_read = '/list_h_3/' + log_file
    file_name_write = '/list_h_6/' + log_file
    name_of_h = log_file.split('.')[1]
    h_value = int(name_of_h[1:])
    name_of_user = log_file.split('.')[2]
    user_id = int(name_of_user[1:])
    with client_hdfs.read(file_name_read, encoding='utf-8') as File:
        with client_hdfs.write(file_name_write, encoding='utf-8') as NewFile:
            df = pd.read_csv(File)
            df['HValue'] = h_value
            df['userid'] = user_id
            df.drop('\tSpeed', axis=1, inplace=True)

#           Save new file
            df.to_csv(NewFile, index=False)


client_hdfs = InsecureClient('http://localhost:9870')

for i in sys.stdin:
    i = i.rstrip()
    process_files(i)
