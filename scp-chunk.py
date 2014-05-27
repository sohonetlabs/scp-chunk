__author__ = 'Pat & Ben'
#This version
#TODO - check if destination exists (overwrite logic - how should this behave
#       scp, just overwrites...)

#Next iteration
#TODO - implement password mode, rather than just shared ssh keys
#TODO - individual md5 check in event of failure / recovery option.
#TODO - classify main block of code.
#TODO - What about remote to local copy....?
#TODO - Implement cipher setting checking

import os
import sys
import argparse
import subprocess
import hashlib
from subprocess import CalledProcessError
from threading import Thread
from Queue import Queue

default_num_threads = 3
default_retries = 0
default_cypher = 'arcfour'
split_file_basename = 'chunk_'

# see: http://goo.gl/kTQMs
SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                       'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                       'zebi', 'yobi'),
}


def bytes2human(n, format='%(value).1f %(symbol)s', symbols='customary'):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0 B'
      >>> bytes2human(0.9)
      '0.0 B'
      >>> bytes2human(1)
      '1.0 B'
      >>> bytes2human(1.9)
      '1.0 B'
      >>> bytes2human(1024)
      '1.0 K'
      >>> bytes2human(1048576)
      '1.0 M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5 Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6 K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6 Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> bytes2human(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, format="%(value).5f %(symbol)s")
      '9.76562 K'
    """
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()
    return format % dict(symbol=symbols[0], value=n)


def human2bytes(s):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776

      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]: 1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i + 1) * 10
    return int(num * prefix[letter])


def spinning_cursor():
    while True:
        for cursor in '|/-\\':
            yield cursor

spinner = spinning_cursor()


def spin(text):

    sys.stdout.write(text + ' ' + spinner.next())
    sys.stdout.flush()
    back_spc = (len(text) + 2) * '\b'
    sys.stdout.write(back_spc)


def split_file_and_md5(file_name, prefix, max_size, padding_width=5,
                       buffer=1024 * 1024 * 5):

    chunks = []
    file_md5 = hashlib.md5()
    (path, file_name_part) = os.path.split(file_name)
    with open(file_name, 'r+b') as src:
        suffix = 0
        while True:
            chunk_name = os.path.join(path, prefix + '.%0*d' % \
                                      (padding_width, suffix))
            with open(chunk_name, 'w+b') as tgt:
                chunk_md5 = hashlib.md5()
                written = 0
                while written <= max_size:
                    data = src.read(buffer)
                    file_md5.update(data)
                    chunk_md5.update(data)
                    if data:
                        tgt.write(data)
                        written += buffer
                        spin(chunk_name)
                    else:
                        chunks.append((chunk_name, chunk_md5.hexdigest()))

                        return ((file_name, file_md5.hexdigest()), chunks)
                suffix += 1
                chunks.append((chunk_name, chunk_md5.hexdigest()))


class WorkerThread(Thread):

    def __init__(self, file_queue, dst_file,
                 remote_server,
                 cypher):

        Thread.__init__(self)
        self.file_queue = file_queue
        self.dst_file = dst_file
        self.remote_server = remote_server
        self.cypher = cypher

    def run(self):
        while True:
            if self.file_queue.empty():
                return
            else:
                try:
                    (src_file, dest_file, chunk_num, total_chunks, retries) = \
                                    self.file_queue.get(timeout=1)
                    print "Starting chunk: " + src_file + ' ' + \
                          str(chunk_num) + ':' + \
                          str(total_chunks) + \
                          ' remaining ' + \
                          str(self.file_queue.qsize()) + \
                          ' retries ' + str(retries)
                    res = self.upload_chunk(src_file, dest_file)
                    if res:
                        print "Finished chunk: " + src_file + ' ' + \
                              str(chunk_num) + ':' + str(total_chunks) + \
                              ' remaining ' + str(self.file_queue.qsize())
                        self.file_queue.task_done()
                    else:
                        retries = retries - 1
                        if retries > 0:
                            print "Re-queuing failed chunk: " + src_file + \
                                  ' ' + str(chunk_num) + ' retries left ' + \
                                  str(retries)
                            self.file_queue.put((src_file,
                                                 dest_file,
                                                 chunk_num,
                                                 total_chunks,
                                                 retries))
                        else:
                            print "ERROR: FAILED to upload " + src_file + \
                                  ' ' + str(chunk_num)
                        self.file_queue.task_done()
                except Exception as inst:
                    print 'ERROR: in uploading in tread'
                    retries = retries - 1
                    if retries > 0:
                        print "Re-queuing failed chunk: " + src_file + ' ' + \
                              str(chunk_num) + ' retries left ' + str(retries)
                        self.file_queue.put((src_file,
                                             dest_file,
                                             chunk_num,
                                             total_chunks,
                                             retries))
                    else:
                        print "FAILED to upload " + src_file + ' ' + \
                              str(chunk_num)
                    self.file_queue.task_done()

    def upload_chunk(self, src_file, dest_file):
        try:
            subprocess.check_call(['scp', '-c' + self.cypher, '-q',
                                   '-oBatchMode=yes', src_file,
                                   self.remote_server + ':' + dest_file])
        except CalledProcessError as e:
            return False
        return True


def get_file_md5(filename, buffer_size=1024 * 1024 * 2):
    """Return the hex digest of a file without loading it all into memory"""
    fh = open(filename)
    digest = hashlib.md5()
    while 1:
        buf = fh.read(buffer_size)
        if buf == "":
            break
        digest.update(buf)
    fh.close()
    return str(digest.hexdigest()).lower()


def main():
    #Read in arguments
    parser = argparse.ArgumentParser(description='Chunk a file and then kick'
                                                 ' off multiple SCP threads.'
                                                 'Speeds up transfers over '
                                                 'high latency links')
    parser.add_argument('-c', '--cypher',
                        help='cypher use with from transfer see: ssh',
                        default=default_cypher,
                        required=False)
    parser.add_argument('-s', '--size',
                        help='size of chunks to transfer.',
                        default='500M',
                        required=False)
    parser.add_argument('-r', '--retries',
                        help='number of times to retry transfer.',
                        default=default_retries,
                        required=False,
                        type=int)
    parser.add_argument('-t', '--threads',
                        help='number of threads (default ' +
                        str(default_num_threads) + ')',
                        default=default_num_threads,
                        required=False,
                        type=int)
    #parser.add_argument('-p','--password', help='password',required=False)
    parser.add_argument('src', help='source file')
    parser.add_argument('srv',
                        help='remote server and user if required'
                             ' e.g foo@example.com')
    parser.add_argument('dst',
                        help='directory (if remote home dir then specify . )')

    args = parser.parse_args()

    ssh_crypto = args.cypher
    try:
        chunk_size = human2bytes(args.size)
    except ValueError as e:
        print 'Invalid chunk size ' + str(e)
        exit(1)
    num_threads = args.threads
    src_file = args.src
    dst_file = args.dst
    remote_server = args.srv
    retries = args.retries

    (dest_path, dest_filename) = os.path.split(dst_file)
    (src_path, src_filename) = os.path.split(src_file)
    remote_dest_file = os.path.join(dest_path, src_filename)
    remote_chunk_files = []

    # Check args for errors + instantiate variables.
    if not os.path.exists(src_file):
        print 'Error: Source file does not exist', src_file
        exit(1)

    # Split file and calc the file md5
    split_cmd = ''
    print "spliting file"
    spinner = spinning_cursor()
    sys.stdout.write(spinner.next())
    sys.stdout.flush()
    sys.stdout.write('\b')
    (src_file_info, chunk_infos) = split_file_and_md5(src_file,
                                                      split_file_basename,
                                                      chunk_size)
    src_file_md5 = src_file_info[1]
    print "uploading MD5 (%s) checksum to remote site" % src_file_md5
    try:
        checksum_filename = src_file+'.md5'
        dest_checksum_filename = os.path.join(dest_path,src_filename+'.md5')
        with open(checksum_filename,'w+') as checksum_file:
            checksum_file.write(src_file_md5 +' '+src_filename)
        subprocess.check_call(['scp', '-c' + ssh_crypto, '-q',
                                '-oBatchMode=yes', src_file,
                                remote_server + ':' + \
                                dest_checksum_filename])
    except CalledProcessError as e:
        print(e.returncode)
        print "ERROR: Couldn't connect to remote server."
        exit(1)

    # Fill the queue of files to transfer
    q = Queue()
    chunk_num = 1
    total_chunks = len(chunk_infos)
    for (src_chunk_filename, chunk_md5) in chunk_infos:
        # create destination path
        (path, src_filename) = os.path.split(src_chunk_filename)
        dest_chunk_filename = os.path.join(dest_path, src_filename)
        remote_chunk_files.append((src_chunk_filename,
                                   dest_chunk_filename,
                                   chunk_md5))
        q.put((src_chunk_filename,
               dest_chunk_filename,
               chunk_num,
               total_chunks,
               retries))
        chunk_num = chunk_num + 1

    # Kick off threads
    print "starting transfers"
    for i in range(num_threads):
        t = WorkerThread(q, dst_file, remote_server, ssh_crypto)
        t.daemon = True
        t.start()
    q.join()
    #join the chunks back together and check the md5
    print "re-assembling file at remote end"
    chunk_count = 0

    for (chunk_filename, chunk_md5) in chunk_infos:
        (path, remote_chunk_filename) = os.path.split(chunk_filename)

        remote_chunk_file = os.path.join(dest_path, remote_chunk_filename)

        spin('processing ' + remote_chunk_filename)
        if chunk_count:
            cmd = remote_chunk_file + '>> ' + remote_dest_file
        else:
            #truncate if the first chunk
            cmd = remote_chunk_file + '> ' + remote_dest_file

        subprocess.call(['ssh', remote_server, 'cat', cmd])
        chunk_count += 1

    print
    print 're-assembled'
    print "checking remote file checksum"
    try:
        # use openssl to be cross platform (OSX,Linux)
        checksum = subprocess.check_output(['ssh', remote_server, 'openssl',
                                            'md5', remote_dest_file])
        # MD5(2GB.mov)= d8ce4123aaacaec671a854f6ec74d8c0
        if checksum.find(src_file_md5) != -1:
            print 'PASSED checksums match'
        else:
            print 'ERROR: MD5s do not match local(' + src_file_md5 + \
                  ') != (' + checksum.strip() + ')'
            print '       File uploaded with errors - MD5 did not match.'
            print '       local and remote chunks not cleared up'
            exit(1)
    except CalledProcessError as e:
        print(e.returncode)
        print 'ERROR: File uploaded with errors - MD5 did not match.'
        print '       local and remote chunks not cleared up'
        exit(1)

    # clean up
    print "cleaning up"
    print "removing file chunks"
    for (local_chunk, remote_chunk, chunk_md5) in remote_chunk_files:
        spin("removing file chunk " + local_chunk)
        os.remove(local_chunk)
        try:
            subprocess.call(['ssh', remote_server, 'rm', remote_chunk])
        except CalledProcessError as e:
            print(e.returncode)
            print 'ERROR: failed to remove remote chunk ' + remote - chunk
    print ''
    print "transfer complete"
    exit(0)

main()
