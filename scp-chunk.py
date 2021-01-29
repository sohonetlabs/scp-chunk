__author__ = 'Patrick Sumby and Ben Roeder'

import os
import sys
import argparse
import subprocess
import hashlib
import time
import re
from subprocess import CalledProcessError
from threading import Thread
from queue import Queue

winPath = re.compile('^(\w)\:[\\/](.*)$', re.IGNORECASE)

default_num_threads = 3
default_retries = 0
default_cypher = 'aes128-cbc'
split_file_basename = 'chunk_'

INTERVALS = [1, 60, 3600, 86400, 604800, 2419200, 29030400]
NAMES = [('second', 'seconds'),
         ('minute', 'minutes'),
         ('hour', 'hours'),
         ('day', 'days'),
         ('week', 'weeks'),
         ('month', 'months'),
         ('year', 'years')]


def humanize_time(amount, units):
    '''
      Divide `amount` in time periods.
      Useful for making time intervals more human readable.

      >>> humanize_time(173, "hours")
      [(1, 'week'), (5, 'hours')]
      >>> humanize_time(17313, "seconds")
      [(4, 'hours'), (48, 'minutes'), (33, 'seconds')]
      >>> humanize_time(90, "weeks")
      [(1, 'year'), (10, 'months'), (2, 'weeks')]
      >>> humanize_time(42, "months")
      [(3, 'years'), (6, 'months')]
      >>> humanize_time(500, "days")
      [(1, 'year'), (5, 'months'), (3, 'weeks'), (3, 'days')]
   '''
    result = []

    unit = list(map(lambda a: a[1], NAMES)).index(units)
    # Convert to seconds
    amount = amount * INTERVALS[unit]

    for i in range(len(NAMES) - 1, -1, -1):
        a = amount // INTERVALS[i]
        if a > 0:
            result.append((a, NAMES[i][1 % a]))
            amount -= a * INTERVALS[i]

    return result


def humanize_time_to_string(time):
    time_str = ''
    for (t, units) in time:
        time_str += str(t) + ' ' + str(units) + ' '
    return time_str

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
    for _, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret {0!r}".format(init))
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

    sys.stdout.write(text + ' ' + next(spinner))
    sys.stdout.flush()
    back_spc = (len(text) + 2) * '\b'
    sys.stdout.write(back_spc)


def split_file_and_md5(file_name, prefix, max_size, padding_width=5,
                       buff=1024 * 1024 * 5):

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
                    data = src.read(buff)
                    file_md5.update(data)
                    chunk_md5.update(data)
                    if data:
                        tgt.write(data)
                        written += buff
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
                    print("Starting chunk: " + src_file + ' ' + \
                          str(chunk_num) + ':' + \
                          str(total_chunks) + \
                          ' remaining ' + \
                          str(self.file_queue.qsize()) + \
                          ' retries ' + str(retries))
                    res = self.upload_chunk(src_file, dest_file)
                    if res:
                        print("Finished chunk: " + src_file + ' ' + \
                              str(chunk_num) + ':' + str(total_chunks) + \
                              ' remaining ' + str(self.file_queue.qsize()))
                        self.file_queue.task_done()
                    else:
                        retries = retries - 1
                        if retries > 0:
                            print("Re-queuing failed chunk: " + src_file + \
                                  ' ' + str(chunk_num) + ' retries left ' + \
                                  str(retries))
                            self.file_queue.put((src_file,
                                                 dest_file,
                                                 chunk_num,
                                                 total_chunks,
                                                 retries))
                        else:
                            print("ERROR: FAILED to upload " + src_file + \
                                  ' ' + str(chunk_num))
                        self.file_queue.task_done()
                except Exception as _:
                    print('ERROR: in uploading in tread')
                    retries = retries - 1
                    if retries > 0:
                        print("Re-queuing failed chunk: " + src_file + ' ' + \
                              str(chunk_num) + ' retries left ' + str(retries))
                        self.file_queue.put((src_file,
                                             dest_file,
                                             chunk_num,
                                             total_chunks,
                                             retries))
                    else:
                        print("FAILED to upload " + src_file + ' ' + \
                              str(chunk_num))
                    self.file_queue.task_done()

    def upload_chunk(self, src_file, dest_file):
        try:
            # subprocess.check_call(['scp', '-c' + self.cypher, '-q',
            #                        '-oBatchMode=yes', '-oConnectTimeout=30', src_file,
            #                        self.remote_server + ':' + dest_file])
            if winPath.match(src_file):
                src_file = winPath.sub(r'/\g<1>/\g<2>', src_file)
            src_file = src_file.replace("\\", "/")
            subprocess.check_call(['rsync', '-Ptz', '--inplace', '--rsh=ssh',
                                   '--timeout=30', src_file,
                                   self.remote_server + ':' + dest_file])
        except CalledProcessError as _:
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


def human_sizes(size):
    try:
        chunk_size = human2bytes(size)
    except ValueError as _:
        msg = "Invalid size " + str(size) + " try 1G"
        raise argparse.ArgumentTypeError(msg)
    return size


def main():
    start_time = time.time()
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
                        required=False,
                        type=human_sizes)
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
        print('Invalid chunk size ' + str(e))
        exit(1)
    num_threads = args.threads
    src_file = args.src
    dst_file = args.dst
    remote_server = args.srv
    retries = args.retries

    (dest_path, _) = os.path.split(dst_file)
    if dest_path == "":
        dest_path = "~/"
    (_ , src_filename) = os.path.split(src_file)
    remote_dest_file = os.path.join(dest_path, src_filename)
    remote_chunk_files = []

    # Check args for errors + instantiate variables.
    if not os.path.exists(src_file):
        print('Error: Source file does not exist', src_file)
        exit(1)
    if not os.path.isfile(src_file):
        print('Error: Source is not a file', src_file)
        exit(1)

    src_file_size = os.stat(src_file).st_size
    # Split file and calc the file md5
    local_chunk_start_time = time.time()
    print("spliting file")
    spinner = spinning_cursor()
    sys.stdout.write(next(spinner))
    sys.stdout.flush()
    sys.stdout.write('\b')
    (src_file_info, chunk_infos) = split_file_and_md5(src_file,
                                                      src_filename,
                                                      chunk_size)
    src_file_md5 = src_file_info[1]
    local_chunk_end_time = time.time()
    print("uploading MD5 ({0!s}) checksum to remote site".format(src_file_md5))
    try:
        checksum_filename = src_file + '.md5'
        dest_checksum_filename = os.path.join(dest_path, src_filename + '.md5')
        with open(checksum_filename, 'w+') as checksum_file:
            checksum_file.write(src_file_md5 + ' ' + src_filename)
        print('copying ' + src_file + ' to ' + dest_checksum_filename)
        subprocess.check_call(['scp', '-c' + ssh_crypto, '-q',
                                '-oBatchMode=yes', checksum_filename,
                                remote_server + ':' + \
                                dest_checksum_filename])
    except CalledProcessError as e:
        print(e.returncode)
        print("ERROR: Couldn't connect to remote server.")
        exit(1)

    # Fill the queue of files to transfer
    q = Queue()
    chunk_num = 1
    total_chunks = len(chunk_infos)
    for (src_chunk_filename, chunk_md5) in chunk_infos:
        # create destination path
        (_, src_filename) = os.path.split(src_chunk_filename)
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
    transfer_start_time = time.time()
    print("starting transfers")
    for i in range(num_threads):
        t = WorkerThread(q, dst_file, remote_server, ssh_crypto)
        t.daemon = True
        t.start()
    q.join()
    transfer_end_time = time.time()

    #join the chunks back together and check the md5
    print("re-assembling file at remote end")
    remote_chunk_start_time = time.time()
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
    print()
    print('re-assembled')
    remote_chunk_end_time = time.time()

    print("checking remote file checksum")
    remote_checksum_start_time = time.time()
    try:
        # use openssl to be cross platform (OSX,Linux)
        checksum = subprocess.check_output(['ssh', remote_server, 'openssl',
                                            'md5', remote_dest_file])
        # MD5(2GB.mov)= d8ce4123aaacaec671a854f6ec74d8c0
        print("checksum.find(src_file_md5):" + checksum.decode('utf-8').strip() + " - " + src_file_md5)
        if checksum.decode('utf-8').strip().find(src_file_md5) != -1:
            print('PASSED checksums match')
        else:
            print('ERROR: MD5s do not match local(' + src_file_md5 + \
                  ') != (' + checksum.strip() + ')')
            print('       File uploaded with errors - MD5 did not match.')
            print('       local and remote chunks not cleared up')
            exit(1)
    except CalledProcessError as e:
        print(e.returncode)
        print('ERROR: File uploaded with errors - MD5 did not match.')
        print('       local and remote chunks not cleared up')
        exit(1)

    remote_checksum_end_time = time.time()
    # clean up
    print("cleaning up")
    print("removing file chunks")
    for (local_chunk, remote_chunk, chunk_md5) in remote_chunk_files:
        spin("removing file chunk " + local_chunk)
        os.remove(local_chunk)
        try:
            subprocess.call(['ssh', remote_server, 'rm', remote_chunk])
        except CalledProcessError as e:
            print(e.returncode)
            print('ERROR: failed to remove remote chunk ' + remote_chunk)
    print('')
    print("transfer complete")
    end_time = time.time()
    print("-" * 80)
    print("file size              :" + bytes2human(src_file_size) + "B")
    print("transfer rate          :" + bytes2human(src_file_size /
                                                   int(transfer_end_time - \
                                                   transfer_start_time)) + "B/s")
    print("                       :" + bytes2human((src_file_size * 8) /
                                                  int(transfer_end_time - \
                                                  transfer_start_time)) + "b/s")
    print("transfer time          :" + str(humanize_time_to_string( \
                                           humanize_time( \
                                           int(transfer_end_time - \
                                           transfer_start_time),
                                           "seconds"))))
    print("local chunking time    :" + str(humanize_time_to_string( \
                                           humanize_time( \
                                           int(local_chunk_end_time - \
                                           local_chunk_start_time),
                                           "seconds"))))
    print("remote reassembly time :" + str(humanize_time_to_string( \
                                           humanize_time( \
                                           int(remote_chunk_end_time - \
                                           remote_chunk_start_time),
                                           "seconds"))))
    print("remote checksum time   :" + str(humanize_time_to_string(
                                           humanize_time( \
                                           int(remote_checksum_end_time - \
                                           remote_checksum_start_time), \
                                           "seconds"))))
    print("total transfer rate    :" + bytes2human(src_file_size / \
                                                   int(end_time - \
                                                       start_time)) + "B/s")
    print("                       :" + bytes2human((src_file_size * 8) / \
                                                   int(end_time - \
                                                       start_time)) + "b/s")
    print("total time             :" + str(humanize_time_to_string( \
                                           humanize_time(int(end_time - \
                                           start_time), "seconds"))))

    exit(0)

main()
