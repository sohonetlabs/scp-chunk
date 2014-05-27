scp-chunk
===================
##Why ?
For transferring files over long latency links. Depending on the TCP/IP stack and the version of ssh installed latency can limit the speed that a single transfer will achieve, on a per connection basis. To work around this scp-chunk transfers multiple chunks at the same time.

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

##Example

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

## Thankyou
[Bytes-to-human / human-to-bytes converter](http://code.activestate.com/recipes/578019-bytes-to-human-human-to-bytes-converter/)