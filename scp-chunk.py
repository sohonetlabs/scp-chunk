__author__ = 'pat'

#This version
#TODO - check if destination exists (overwrite logic - how should this behave - scp, just overwrites...)

#Next iteration
#TODO - individual md5 check in event of failure / recovery option.
#TODO - classify main block of code.
#TODO - What about remote to local copy....?
#TODO - Implement cipher setting



import os
import argparse
import subprocess
from subprocess import CalledProcessError, check_output
import glob
import hashlib

import time
import random


from threading import Thread

class MyThread(Thread):

    def __init__(self, file , dst_file, remote_server):
        Thread.__init__(self)
        self.file = file
        self.dst_file = dst_file
        self.remote_server = remote_server

    def run(self):
        try:
            #subprocess.call(['scp', '-carcfour', '-f' , self.file , self.remote_server+':'+self.dst_file])
            subprocess.call(['scp', '-carcfour', '-q' ,'-oBatchMode=yes' , self.file , self.remote_server+':'+self.dst_file])
        except CalledProcessError as e:
            print(e.returncode)
            print "ERROR: "+file +" upload failed"
            return False
            #exit(1)
        print "Uploaded chunk: " + self.file
        return True


def upload_chunk((file , dst_file, remote_server)):
    try:
        #subprocess.call(['scp', '-carcfour', '-f' , self.file , self.remote_server+':'+self.dst_file])
        time.sleep(10 * random.random())
        subprocess.call(['scp', '-carcfour', '-q' ,'-oBatchMode=yes' , file , remote_server+':'+dst_file])
    except CalledProcessError as e:
        print(e.returncode)
        print "ERROR: "+file +" upload failed"
        #exit(1)
    print "Uploaded chunk: " + file






default_num_chunks=10
default_chunk_size=200000 #Bytes
default_num_streams=5
split_file_basename='split_parts'




def getMD5OfFile(filename):
    """Return the hex digest of a file without loading it all into memory"""
    fh = open(filename)
    digest = hashlib.md5()
    while 1:
        buf = fh.read(1024*1024*1)
        if buf == "":
            break
        digest.update(buf)
    fh.close()
    return str(digest.hexdigest()).lower()

def main():

    #Read in arguments
    parser = argparse.ArgumentParser(description='Chunk a file and then kick off multiple SCP threads.'
                                     'Speeds up transfers over high latency links ')
    parser.add_argument('-c', help='crypto to send to SSH')
    parser.add_argument('-C', help='number of chunks (default '+ str(default_num_chunks) + ')')
    parser.add_argument('-s', help='size of chunks (Bytes)')
    #TODO - Need implement number of streams
    parser.add_argument('src', help='source file')
    parser.add_argument('srv', help='remote server and user if required')
    parser.add_argument('dst', help='directory (if remote home dir then specify .)')

    args = parser.parse_args()
    ssh_crypto = args.c
    num_chunks = args.C
    chunk_size = args.s
    src_file = args.src
    dst_file = args.dst
    remote_server = args.srv
    print args


    # Check args for errors + instansiate variables.
    if not os.path.exists(src_file):
        print 'source file does not exist', src_file
        exit(1)

    if num_chunks and chunk_size:
        print 'specify a chunk size OR a number of chunks'


    # Take MD5
    print "taking MD5 of file"
    src_file_md5 = getMD5OfFile(src_file)

    try:
        subprocess.call(['ssh' , remote_server , 'echo', src_file_md5+'\ \ '+src_file+'>'+src_file+'.md5'])
    except CalledProcessError as e:
        print(e.returncode)
        print "ERROR: Couldn't connect to remote server."
        exit(1)


    # TODO - Get error code at this point to fail out if command fails. try/catch
    print "uploading md5 to remote site"

    # Split file
    split_cmd = ''
    print "spliting file"
    if chunk_size != None:
        print "Using manually defined chunk size"
        split_cmd = ['/usr/bin/split' ,'-b'+str(chunk_size) , src_file , split_file_basename]
    else:
        if num_chunks != None:
            print "Using manually defined number of chunks"
            statinfo = os.stat(src_file)
            chunk_size = statinfo.st_size / num_chunks
            split_cmd = ['/usr/bin/split' ,'-b'+str(chunk_size) , src_file , split_file_basename]
        else:
            print "Using default number of chunks"
            statinfo = os.stat(src_file)
            chunk_size = statinfo.st_size / default_num_chunks
            print statinfo.st_size
            print chunk_size
            split_cmd = ['/usr/bin/split' ,'-b'+str(chunk_size) , src_file , split_file_basename]
    subprocess.call(split_cmd)


    # Kick off threads
    file_list = glob.glob(split_file_basename+'*')
    threads = []
    for file in file_list:
        T = MyThread(file, dst_file , remote_server)
        threads.append(T)
        T.start()
        print "Started chunk: " + file
    for i in threads:
        i.join()



    # from multiprocessing import Pool
    # pool = Pool(processes=3)              # start worker processes
    # print pool.map(upload_chunk, [(f, dst_file, remote_server) for f in file_list])

job_q = Queue()

def upload(job_q, out_q):
 while True:
  try
    f = job_q.get(timeout=1)
    res = upload_chunk(f,d,r)
    out_q.put(res)
  except Queue.Empty:
    out_q.put(None)
    return


threads = [ Thread() ]





    print "Re-assembling file at remote end"
    subprocess.call(['ssh' , remote_server , 'cat', split_file_basename+'*>'+src_file, ';', 'rm', split_file_basename+'*'])
    print "Checking file"
    try:
        subprocess.check_call(['ssh' , remote_server , 'md5sum', '-c' , src_file+'.md5'])
    except CalledProcessError as e:
        print(e.returncode)
        print "ERROR: File uploaded with errors - MD5 did not match."
        exit(1)

    print "Cleaning up"
    for file in file_list:
        subprocess.call(['rm', file])
    print "Success"

def f(x):
    return x*x


main()