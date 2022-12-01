import glob
import sys
from flask import g
import pandas as pd
import os
import time
import icecream as ic
import argparse
from tomlkit import string

parser = argparse.ArgumentParser()
parser.add_argument('context', type=str, help="Context to recall")
parser.add_argument("-last", "--last", help="Size", type=str)
parser.add_argument("-granularity", "--granularity", help="Size", type=str)
parser.add_argument("--start", "-start", help="Size", type=str)
parser.add_argument("--end", "-end", help="Size", type=str)
parser.add_argument("--cpu", help="Size", type=str)
parser.add_argument("--process", help="Size", type=str)
parser.add_argument("--pid", help="Size", type=str)
parser.add_argument("-disk_name", help="Size", type=str)
parser.add_argument("--mnt_point", help="Size", type=str)
parser.add_argument("--graph", help="Size", type=str)
parser.add_argument("--corr", help="Size", type=str)
args = parser.parse_args()
data_dir = 'psaz'
global g_value
g_value = 'mean'

def convert_to_epoch(dt_ti):
    epoch = int(time.mktime(time.strptime(dt_ti+":00", '%Y-%m-%d:%H:%M:%S')))
    return epoch


def seg_find(stime,etime,last,granularity):
    """
    Given a start time, end time, last time, and granularity, return a list of tuples of the form
    (start,end) where start and end are the start and end times of each segment
    
    :param stime: start time of the segment
    :param etime: end time of the segment
    :param last: the last time in the data set
    :param granularity: the time interval in seconds
    """
    if stime is not None:
        if etime is not None:
            stime = convert_to_epoch(stime)
            etime = convert_to_epoch(etime)
            if granularity is None: granularity = '5m'
            print(map)
    if last is not None:
        if granularity is None: granularity = '5m'
        etime = int(time.time())
        if "".join(filter(lambda x: not x.isdigit(), last)) == 'h':
            multiplier = 3600
        elif "".join(filter(lambda x: not x.isdigit(), last)) == 'm':
            multiplier = 60
        elif "".join(filter(lambda x: not x.isdigit(), last)) == 'd':
            multiplier = 86400
        else:
            multiplier = 1
        number = int("".join(filter(lambda x: x.isdigit(), last)))
        etime = int(time.time())
        stime = etime - (number * multiplier)
    if last is None:
        if etime is None:
            if stime is None:
                stime = int(time.time()) - 300
                etime = int(time.time())
                if granularity is None: granularity = '5m'
    return stime,etime,granularity


def filter_data(context, data, condition):
    global g_value
    """
    It takes in the context, data, and condition and returns the dataframe with the columns that are
    relevant to the context.
    
    :param context: The context of the data you want to retrieve
    :param data: The dataframe that contains the data to be filtered
    :param condition: This is the condition that you want to filter the data on. For example, if you
    want to filter the data on a particular CPU, you can specify the CPU number here
    :return: The data is being returned in a dataframe format.
    """
    if context == 'cpu':
        return data[['ctx_switches','idle','interrupts','iowait','irq','system','user','cpucore','total','syscalls','time']]
    if context == 'diskio':
        try:
            data = data.query('disk_name == @condition')
        except:
            "No disk name specified"
        g_value = 'disk'
        return data[['disk_name','read_bytes','read_count','write_bytes','write_count','time']]
    if context == 'mem':
        return data[['active','available','buffers','cached','free','inactive','percent','shared','total','used','time']]
    if context == 'percpu':
        try:
            condition = int(condition)
            #check if cpu = condition using query
            data = data.query('cpu_number == @condition')
            return data[['cpu_number','idle','iowait','irq','system','total','user','time']]
        except:
            "No cpu specified"
        return data[['cpu_number','idle','iowait','irq','system','total','user','time']]
    if context == 'memswap':
        return data[['free','percent','sin','sout','total','used','time']]
    if context == 'processlist':
        if condition is not None:
            try:
                condition = str(condition)
                if condition.isdigit():
                    condition = int(condition)
                    data = data.query('pid == @condition')
                else:
                    data = data.query('name == @condition')
            except:
                "No process specified"
        print("we did try")
        g_value = 'process'
        return data
    if context == 'processcount':
        return data[['pid_max','running','sleeping','thread','total','time']]
    if context == 'load':
        return data[['min1','min15','min5','cpu_core','time']]
    if context == 'fs':
        #print(data)
        #
        print(condition)
        
        try:
            if condition is not None:
                data = data.query('mnt_point == @condition')
            else:
                print("No mount point specified")
        except:
            "No mount point specified"
        g_value = 'fs'
        return data
        # find initial and last point of data using iloc
        #tmp[['fstype','mnt_point']] = data[['fs_type','mnt_point']]
        
    if context == 'sensors':
        g_value = 'sensors'
        return data[['label','type','unit','max','min','average','time']]


def collect_data(context, stime , etime, granularity, last,special):
    """
    This function collects data from the API and returns a dataframe with the data
    
    :param context: the context object
    :param stime: start time
    :param etime: end time of the data to be collected
    :param granularity: The time interval between each data point
    :param last: the last time the function was called
    :param special: a list of special symbols, like ['.DJI','.IXIC','.INX']
    """
    
    stime,etime,gran = seg_find(stime,etime,last,granularity)
    data = fetch_start_end(context, stime, etime)
    #print(data)
    data = filter_data(context,data,special)
    
    data = granularize(data, gran)
    #print(data)
    return data
    
    
def fetch_start_end(context, epoch_s, epoch_e):
    """
    It takes in the start and end epochs, and returns the data from that time period in the form of a dataframe
    
    :param context: The context to be analyzed ie. cpu, diskio etc
    :param epoch_s: start epoch or time
    :param epoch_e: end epoch or time
    """
    data = pd.DataFrame()
    map = pd.read_table(os.path.join(data_dir,'mapfile.txt'), sep=':', skiprows=1,header=None)
    map.columns = ['epoch', 'time']
    map.set_index('epoch', inplace=True)
    map_u = map.query('time >= @epoch_s & time <= @epoch_e')
    for i in map_u.index.tolist():
        file = os.path.join(data_dir,'psaz_data.'+str(i),context+'.csv')
        if not os.path.exists(file):
            continue
        df = pd.read_csv(file)
        df['time'] = df['iteration&time'].apply(lambda x: int(float(x.split(':')[1])))
        data = pd.concat([data, df.query('time >= @epoch_s & time <= @epoch_e')])

    return data
def g_value_manager(data):
    global g_value
    """
    If the g_value is mean, return the mean of the data. If the g_value is fs, return the first and last
    rows of the data. If the g_value is process, return the data with the io_counters column converted
    to a list and the readio and writeio columns calculated
    
    :param data: the dataframe that is passed to the function
    :return: the dataframe with the columns specified in the if statement.
    """
    if data.empty:
            print("No data")
            return data
        
    if g_value == 'mean':
        return data.mean()
    if g_value == 'fs':
        print("data : ",data)
        tmp = pd.DataFrame()
        
        try:
            dataframelist = [data['fs_type'].iloc[0],data['mnt_point'].iloc[0],data['percent'].iloc[0], data['size'].iloc[0], data['free'].iloc[0], data['used'].iloc[-1]-data['used'].iloc[0], data['percent'].iloc[-1], data['size'].iloc[-1], data['free'].iloc[-1]]
            tmp = pd.DataFrame([dataframelist], columns=['fs_type','mnt_point','begin_percentage','begin_size','begin_free','used','end_percentage','end_size','end_free'])
        except:
            print("Error")
        
        return tmp
        #return tmp[['begin_percentage','begin_size','begin_free','used','end_size','end_free','end_percentage']]
    if g_value == 'process':

        #subtract second element from list io_counters[2] from 1 after converting to list
        data['io_counters'] = data['io_counters'].apply(lambda x: x.strip('][').split(', '))
        data['readio'] = data['io_counters'].apply(lambda x: int(x[0])-int(x[2]))
        data['writeio'] = data['io_counters'].apply(lambda x: int(x[1])-int(x[3]))
        return data[['cmdline','cpu_percent','cpu_times','readio','writeio','memory_info','memory_percent','name','num_threads','status','time']]
    if g_value == 'disk':
        dataframelist = [data['read_bytes'].sum(),data['write_bytes'].sum(),data['read_count'].sum(),data['write_count'].sum(), data['disk_name'].iloc[0], data['time'].iloc[0]]
        tmp = pd.DataFrame([dataframelist], columns=['read_bytes','write_bytes','read_count','write_count','disk_name','time'])
        return data[['disk_name','read_bytes','read_count','write_bytes','write_count','time']]
    if g_value == 'sensors':
        print(data)
        tmp1 = data[['label','type','unit','max','min','average','time']]
        for i in tmp1.groups.keys():
            print(tmp1.get_group(i))
            a = tmp1.get_group(i)
            min = a[a['value'] == a['value'].min()]['value','time']
            max = a[a['value'] == a['value'].max()]['value','time']
            avg = a['average'] = a['value'].mean()
            tmp1 = tmp1.append({'label':i,'type':a['type'].iloc[0],'unit':a['unit'].iloc[0],'max':max,'min':min,'average':avg,'time':a['time'].iloc[0]}, ignore_index=True)
        print(data)
        return tmp1
    
def granularize(data, granularity):
    """
    It takes a list of data and a granularity, and returns compressed list where datapoints have been 
    normalised by their granularity intervals
    
    :param data: A dataframe of data
    :param granularity: Interval to find the mean between data points
    """
    
    if "".join(filter(lambda x: not x.isdigit(), granularity)) == 'h':
        multiplier = 3600
    elif "".join(filter(lambda x: not x.isdigit(), granularity)) == 'm':
        multiplier = 60
    elif "".join(filter(lambda x: not x.isdigit(), granularity)) == 'd':
        multiplier = 86400
    else:
        multiplier = 1 #considering to be seconds
    
    # find the number in granularity
    number = int("".join(filter(lambda x: x.isdigit(), granularity)))
    
    interval = int(number * multiplier)
    output = pd.DataFrame()
    high = int(float(data.iloc[-1]['time']))
    low = int(float(high - interval))
    for i in range(0,len(data)):
        tmp = data.query('time > @low and time < @high')
        if tmp.empty: break
        tmp = g_value_manager(tmp)
        output = output.append(tmp,ignore_index=True)
        high -= interval
        low -= interval
    return output
    

def present_data(data, granularity, argument, argument_value):

    output = pd.DataFrame()
      #output = operations(output)88
    output = granularize(output, granularity)
    
    
        
    
    

    
    
    
    
    
    
    
#try taking in command line arguments where possible
arguments = {'context':args.context, 'last':args.last, 'granularity':args.granularity, 'start':args.start, 'end':args.end, 'cpu':args.cpu, 'process':args.process, 'pid':args.pid, 'disk_name':args.disk_name, 'mnt_point':args.mnt_point, 'graph':args.graph, 'corr':args.corr}
#collect_data(arguments['context'], arguments['start'], arguments['end'], arguments['granularity'], arguments['last'], 'psaz')

if arguments['pid'] is None:
    if arguments['mnt_point'] is None:
        if arguments['disk_name'] is None:
            if arguments['cpu'] is None:
                if arguments['process'] is None:
                    print('No special arguments')
                    option = None
                else:
                    option = arguments['process']
            else:
                option = arguments['cpu']
        else:
            option = arguments['disk_name']
    else:
        option = arguments['mnt_point']
else:
    option = arguments['pid']
try:                    
    print(collect_data(arguments['context'], arguments['start'], arguments['end'], arguments['granularity'], arguments['last'], option))
except:
    print('couldnt find anything')