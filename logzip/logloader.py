# Copyright 2018 The LogPAI Team (https://github.com/logpai).
#
# Licensed under the MIT License:
# Permission is hereby granted, free of charge, to any person obtaining a copy 
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights 
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
# copies of the Software, and to permit persons to whom the Software is 
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in 
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
# SOFTWARE.
# =============================================================================
""" This file implements the formating interface to load log file to dataframe
"""

import sys
import pandas as pd
import re
import multiprocessing as mp
from itertools import groupby, count, chain
import numpy as np
import json
import os
import io
import time

class LogLoader(object):
    def __init__(self, logformat, tmp_dir, n_workers=1):
        if not logformat:
            raise RuntimeError('Logformat is required!')
        self.logformat = logformat.strip()
        self.headers, self.regex = self._generate_logformat_regex(self.logformat)
        self.n_workers = n_workers
        self.tmp_dir = tmp_dir

    def load_to_dataframe(self, log_filepath):
        """ Function to transform log file to dataframe 
        """
        print('Loading log messages to dataframe...')
        t1 = time.time()
        lines = []
        with open(log_filepath, 'r', encoding="utf-8", errors="ignore") as fid:
            lines = fid.readlines()
        print("Total lines {}".format(len(lines)))
        log_messages = []
        failed_messages = {}
        if self.n_workers == 1: 
            log_messages, failed_messages = formalize_message(enumerate(lines), self.regex, self.headers)
        else:
            chunk_size = min(1000000, len(lines) // float(self.n_workers))
            chunks = groupby(enumerate(lines), key=lambda k, line=count(): next(line)//chunk_size)
            log_chunks = [list(chunk) for _, chunk in chunks]
            print('Read %d log chunks in parallel'%len(log_chunks))
            pool = mp.Pool(processes=self.n_workers)
            result_chunks = []
            for cidx, chunk in enumerate(log_chunks, 1):
                result_chunks.append(pool.apply_async(formalize_message,
                                     args=(chunk, self.regex, self.headers)))
            pool.close()
            pool.join()
            for result in result_chunks:
                result = result.get()
                log_messages.extend(result[0])
                failed_messages.update(result[1])
        with open(os.path.join(self.tmp_dir, "failed_logs.json"), "w") as fw:
            json.dump(failed_messages, fw)

        log_dataframe = pd.DataFrame(log_messages, columns=['LineId'] + self.headers)
        success_rate = len(log_messages) / float(len(lines))
        print('Loading {} messages done, loading rate: {:.1%}, failed lines: {}'.format(len(log_messages), success_rate, len(failed_messages)))
        t2 = time.time()
        print('Time taken {:.2f}s'.format(t2-t1))
        return log_dataframe

    def _generate_logformat_regex(self, logformat):
        """ Function to generate regular expression to split log messages
        """
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', '\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                if k == len(splitters) - 2:
                    regex += '(?P<%s>.*?)' % header
                else:
                    regex += '(?P<%s>.*?)' % header
                headers.append(header)
        # print(regex)
        regex = re.compile('^' + regex + '$')
        return headers, regex


def formalize_message(enumerated_lines, regex, headers):
    print("Worker {} processing.".format(os.getpid()))
    log_messages = []
    failed_messages = {}
    for line_count, line in enumerated_lines:
        line = line.strip()
        if not line:
            continue
        line = re.sub(r'[^\x00-\x7F]+', '<NASCII>', line)
        try:
            match = regex.search(line)
            message = [match.group(header) for header in headers]
            message.insert(0, line_count + 1)
            log_messages.append(message)
        except Exception as e:
            failed_messages[line_count] = line
            pass
    print("Worker {} finish.".format(os.getpid()))
    return log_messages, failed_messages