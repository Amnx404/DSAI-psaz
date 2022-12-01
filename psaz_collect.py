import yaml
from yaml.loader import SafeLoader
import pandas as pd
import os
import time
import requests
import glob
import shutil
import sys

def configuration(config_file):
    with open(config_file) as f:
        data = yaml.load(f, Loader = SafeLoader)
        interval = data['data_collection_interval']
        isize = data['data_directory_isize']
        datadir = data['data_dir']
        retention = data['data_retention']
        return interval, isize, datadir, retention
    
def get_data(context):
    response = requests.get('http://localhost:61208/api/3/'+context)
    return process_response(response)


def process_response(response):
    """
    It takes a response object, converts it to JSON, normalizes the JSON, and then converts it to a
    Pandas dataframe
    
    :param response: the response object from the API call
    :return: A dataframe
    """
    data = response.json()
    data = pd.json_normalize(data)
    df = pd.DataFrame.from_dict(data)
    return df

def retention_check(datadir, retention = 100):
    """
    It takes a directory name as input, and removes all the subdirectories in that directory that are
    older than a certain number of days
    
    :param datadir: The directory where the data is stored
    :param retention: The number of days to keep the data, defaults to 100 (optional)
    :return: The high and low values of the data directory
    """
    files = glob.glob(os.path.join(data_dir,'psaz_data.*'))
    if len(files)==0:
        return 1,1
    files.sort(key=lambda x: int(x.split('.')[-1]))
    high = int(files[-1].split('.')[-1])
    low = int(files[0].split('.')[-1])
    #remove folders which are older than retention starting from old
    #if high - low > retention:
    for i in range(low, high - retention+1):
        shutil.rmtree(os.path.join(datadir,'psaz_data.' + str(i)))
        print("Directory removed : "+str(i))
     
    return high,low


#Setting the initial variables in place
config_file = str(sys.argv[1])
interval, i_size, data_dir, retention = configuration(config_file)
dict = {'cpu':pd.DataFrame(),'percpu':pd.DataFrame(), 'mem':pd.DataFrame(), 'memswap':pd.DataFrame(), 'processcount':pd.DataFrame(), 'processlist':pd.DataFrame(),'load':pd.DataFrame(), 'diskio':pd.DataFrame(),'fs':pd.DataFrame(),'sensors':pd.DataFrame()}
obs_no = 1

if os.path.exists(data_dir):
    print('Directory exists')
else:
    os.mkdir(data_dir)
    
    with open(os.path.join(data_dir,'mapfile.txt'), 'w') as f:
        f.write('Mapfile for data directory \n')
        
    print('Directory created')

high , low = retention_check(data_dir,retention) 
i_temp = high+1

while True:
    
    if dict['cpu'].empty:
        for i in dict:
            dict[i] = get_data(i)
            dict[i]['iteration&time'] = str(obs_no)+":"+str(time.time())   
        #cpu =   get_data('cpu')
    else:
        for i in dict:
            data = get_data(i)
            data['iteration&time'] = str(obs_no)+":"+str(time.time())
            dict[i] = pd.concat([dict[i], data], axis=0)
        #cpu = pd.concat([df, get_data('cpu')], axis=0)
    obs_no += 1

        
    #making the interval group dierectory if it dosnt exist already
    if not os.path.exists(os.path.join(data_dir,'psaz_data.' + str(i_temp)) ): 
        os.mkdir(os.path.join(data_dir,'psaz_data.' + str(i_temp)))
        with open(os.path.join(data_dir,'mapfile.txt'), 'a') as f:
            f.write(str(i_temp)+':'+str(time.time())+'\n')
        print('Directory created : '+str(i_temp))
        retention_check(data_dir,retention)

    for i in dict:
        dict[i].to_csv(os.path.join(data_dir,'psaz_data.' + str(i_temp),i+'.csv'), index=False)
    #cpu.to_csv(os.path.join(data_dir,'psaz_data.' + str(i_temp), 'cpu.csv'))  

    
    if len(dict['cpu']) == i_size:
        for i in dict:
            dict[i] = pd.DataFrame()    
        i_temp += 1 
        obs_no = 1  
         
    
    
    time.sleep(interval)