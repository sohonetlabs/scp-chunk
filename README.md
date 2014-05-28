scp-chunk
===================
##Why ?
For transferring files over long latency links. Depending on the TCP/IP stack and the version of ssh installed latency can limit the speed that a single transfer will achieve, on a per connection basis. To work around this scp-chunk transfers multiple chunks at the same time.

Use the system python, without having to install any other python packages!!!! just put this on the machine and go.


##How it works
Split a large file into chunks and then transfer via multiple scp connections.
Then join the chunks back together, check the checksum.
then clean up all the chunks, at the local and remote ends.
It will use at peak twice the disk space of the size of the file to be transferred at each end.

##Requirements
Uses scp to transfer the files to the remote system in parrellel, and expects the user to be pre-keyed to the remote systems.
[see article here on how to set this up]( http://hocuspokus.net/2008/01/ssh-shared-key-setup-ssh-logins-without-passwords/)

###Goal
Use the system python, without having to install any other python packages, just using the programs listed below. 

It is expected that the remote shell will provide access to the following commands :-
####remote system
* [openssl](http://unixhelp.ed.ac.uk/CGI/man-cgi?openssl) usage to calculate checksum: **openssl md5 \<filename>**
* [cat](http://unixhelp.ed.ac.uk/CGI/man-cgi?cat) usage to reassemble chunks: **cat \<filename> >> \<filename>**
* [rm](http://unixhelp.ed.ac.uk/CGI/man-cgi?rm) usage to remove chunks: **rm \<filename>**

#####local system
* [scp](http://unixhelp.ed.ac.uk/CGI/man-cgi?scp) to copy files to remote system.

##Usage


    usage: scp-chunk.py [-h] [-c CYPHER] [-s SIZE] [-r RETRIES] [-t THREADS]  
                       src srv dst  

    Chunk a file and then kick off multiple SCP threads.Speeds up transfers over high latency links  

    positional arguments:
      src                   source file  
      srv                   remote server and user if required e.g foo@example.com
      dst                   directory (if remote home dir then specify . )

    optional arguments:
      -h, --help            show this help message and exit
      -c CYPHER, --cypher CYPHER
                            cypher use with from transfer see: ssh
      -s SIZE, --size SIZE  size of chunks to transfer.
      -r RETRIES, --retries RETRIES
                            number of times to retry transfer.
      -t THREADS, --threads THREADS
                            number of threads (default 3)

##Example output

    python scp-chunk.py  2GB.mov ben@10.110.10.121 . --threads 10

    spliting file
    uploading MD5 (d8ce4123aaacaec671a854f6ec74d8c0) checksum to remote site
    starting transfers
    Starting chunk: chunk_.00000 1:5 remaining 4 retries 0
    Starting chunk: chunk_.00001 2:5 remaining 3 retries 0
    Starting chunk: chunk_.00002 3:5 remaining 2 retries 0
    Starting chunk: chunk_.00003 4:5 remaining 1 retries 0
    Starting chunk: chunk_.00004 5:5 remaining 0 retries 0
    Finished chunk: chunk_.00004 5:5 remaining 0
    Finished chunk: chunk_.00002 3:5 remaining 0
    Finished chunk: chunk_.00001 2:5 remaining 0
    Finished chunk: chunk_.00000 1:5 remaining 0
    Finished chunk: chunk_.00003 4:5 remaining 0
    re-assembling file at remote end
    processing chunk_.00004 -
    re-assembled
    checking remote file checksum
    PASSED checksums match
    cleaning up
    removing file chunks
    removing file chunk chunk_.00004 \
    transfer complete

    PING transfer.example.com (xxx.xxx.xxx.xxx): 56 data bytes
    64 bytes from xxx.xxx.xxx.xxx: icmp_seq=0 ttl=58 time=151.308 ms
    64 bytes from xxx.xxx.xxx.xxx: icmp_seq=1 ttl=58 time=151.264 ms
    64 bytes from xxx.xxx.xxx.xxx: icmp_seq=2 ttl=58 time=151.449 ms
    64 bytes from xxx.xxx.xxx.xxx: icmp_seq=3 ttl=58 time=150.927 ms


    python scp-chunk.py /Stuff/23GBlargefile.mov  ben@ves-us.sohonet.com /Store/ben_test/ --threads 10 --size 1G
    spliting file
    uploading MD5 (5e631de28dd45d1b05952c885a882be1) checksum to remote site
    copying /Stuff/23GBlargefile.mov to /Store/ben_test/23GBlargefile.mov.md5
    starting transfers
    Starting chunk: /Stuff/23GBlargefile.mov.00000 1:29 remaining 28 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00001 2:29 remaining 27 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00002 3:29 remaining 26 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00003 4:29 remaining 25 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00004 5:29 remaining 24 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00005 6:29 remaining 23 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00006 7:29 remaining 22 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00007 8:29 remaining 21 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00008 9:29 remaining 20 retries 0
    Starting chunk: /Stuff/23GBlargefile.mov.00009 10:29 remaining 19 retries 0
    Finished chunk: /Stuff/23GBlargefile.mov.00008 9:29 remaining 19
    Starting chunk: /Stuff/23GBlargefile.mov.00010 11:29 remaining 18 retries 0
    <SNIP>
    Finished chunk: /Stuff/23GBlargefile.mov.00019 20:29 remaining 2
    Starting chunk: /Stuff/23GBlargefile.mov.00027 28:29 remaining 1 retries 0
    Finished chunk: /Stuff/23GBlargefile.mov.00017 18:29 remaining 1
    Starting chunk: /Stuff/23GBlargefile.mov.00028 29:29 remaining 0 retries 0
    Finished chunk: /Stuff/23GBlargefile.mov.00014 15:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00028 29:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00020 21:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00024 25:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00021 22:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00025 26:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00023 24:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00022 23:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00026 27:29 remaining 0
    Finished chunk: /Stuff/23GBlargefile.mov.00027 28:29 remaining 0
    re-assembling file at remote end
    processing 23GBlargefile.mov /
    re-assembled
    checking remote file checksum
    PASSED checksums match
    cleaning up
    removing file chunks
    removing file chunk /Stuff/23GBlargefile.mov.00028 -
    transfer complete
    --------------------------------------------------------------------------------
    file size              :28.2 GB
    transfer rate          :25.7 MB/s
                           :205.9 Mb/s
    transfer time          :18 minutes 43 seconds 
    local chunking time    :10 minutes 35 seconds 
    remote reassembly time :4 minutes 20 seconds 
    remote checksum time   :1 minute 28 seconds 
    total transfer rate    :13.3 MB/s
                           :106.6 Mb/s
    total time             :36 minutes 8 seconds
    
Would be faster is if the disks where not rubbish at the source end. SSD at each end would make it faster to chunk the file.

## Thank you
[Bytes-to-human / human-to-bytes converter](http://code.activestate.com/recipes/578019-bytes-to-human-human-to-bytes-converter/)


[Humanizeize time](https://github.com/liudmil-mitev/experiments/blob/master/time/humanize_time.py)