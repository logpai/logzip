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
""" 
This file implements the regular expression based algorithm for log 
template matching. The algorithm is described in the following paper:
[1] Jieming Zhu, Jingyang Liu, Pinjia He, Zibin Zheng, Michael R. Lyu. 
    "Real-Time Log Event Matching at Scale", XXX, 2018.
"""

from . import logloader
from collections import defaultdict, Counter, OrderedDict
import re
import pandas as pd
import os
import sys
sys.setrecursionlimit(1000000)
from datetime import datetime
import multiprocessing as mp
import itertools
import hashlib
import string
import numpy as np


class PatternMatch(object):
    def __init__(self, tmp_dir, outdir='./result/', n_workers=1, optimized=False, logformat=None):
        self.outdir = outdir
        if not os.path.exists(outdir):
            os.makedirs(outdir) # Make the result directory
        self.tmp_dir = tmp_dir
        self.logformat = logformat
        self.n_workers = n_workers
        self.optimized = optimized

    def match(self, templates, log_filepath=None, log_dataframe=None):
        print('Processing log file: {}...'.format(log_filepath))
        start_time = datetime.now()
        
        if log_dataframe is None:
            loader = logloader.LogLoader(self.logformat, self.tmp_dir)
            log_dataframe = loader.load_to_dataframe(log_filepath)
        # log_dataframe = log_dataframe.head(1)

        templates = self._read_templates(templates)

        print('Building match tree...')
        match_tree = self._build_match_tree(templates)

        print('Matching event templates...')
        if self.optimized:
            match_dict = self.match_event(match_tree, log_dataframe['Content'].drop_duplicates().tolist())
        else:
            match_dict = self.match_event(match_tree, log_dataframe['Content'].tolist())

        log_dataframe['EventTemplate'] = log_dataframe['Content'].map(lambda x: match_dict[x][0])
        log_dataframe['ParameterList'] = log_dataframe['Content'].map(lambda x: match_dict[x][1])
        self.id_map = {tmp:"E"+str(idx) for idx, tmp in enumerate(log_dataframe['EventTemplate'].unique(), 1)} 
        log_dataframe['EventId'] = log_dataframe['EventTemplate'].map(lambda x: self.id_map[x])
#        self._dump_match_result(os.path.basename(log_filepath), log_dataframe)
        match_rate = sum(log_dataframe['EventTemplate'] != 'NoMatch') / float(len(log_dataframe))
        
        print('Matching done, matching rate: {:.1%} [Time taken: {!s}]'.format(match_rate, datetime.now() - start_time))
        return log_dataframe


    def match_event(self, match_tree, log_list):
        if self.n_workers == 1:
            log_template_dict = tree_match(match_tree, log_list)
        else:
            log_template_dict = {}
            pool = mp.Pool(processes=self.n_workers)
            chunk_size = len(log_list) / self.n_workers + 1
            result_chunks = [pool.apply_async(tree_match, args=(match_tree, log_list[i:i + chunk_size]))\
                             for i in range(0, len(log_list), chunk_size)]
            pool.close()
            pool.join()
            map(lambda x: log_template_dict.update(x), [result.get() for result in result_chunks])
        return log_template_dict

    def _build_match_tree(self, templates):
        match_tree = {}
        match_tree["$NO_STAR$"] = {}
        for event_id, event_template in templates:
            # Full match
            if "<*>" not in event_template:
                match_tree["$NO_STAR$"][event_template] = event_template
                continue
            event_template = self._preprocess_template(event_template)
            template_tokens = message_split(event_template)
            if not template_tokens or event_template=="<*>": continue

            start_token = template_tokens[0]
            if start_token not in match_tree:
                match_tree[start_token] = {}
            move_tree = match_tree[start_token]

            tidx = 1
            while tidx < len(template_tokens):
                token = template_tokens[tidx]
                if token not in move_tree:
                    move_tree[token] = {}
                move_tree = move_tree[token]
                tidx += 1
            move_tree["".join(template_tokens)] = (len(template_tokens), template_tokens.count("<*>")) # length, count of <*>
        return match_tree

    def _read_templates(self, templates):
        templates_save = []
        for idx, row in enumerate(templates):
            event_id = "E" + str(idx)
            event_template = row
            templates_save.append((event_id, event_template))
        return templates_save

    def _dump_match_result(self, log_filename, log_dataframe):
        print("Saving {}".format(os.path.join(self.outdir, log_filename + '_structured.csv')))
        print("Saving {}".format(os.path.join(self.outdir, log_filename + '_templates.csv')))
        log_dataframe.to_csv(os.path.join(self.outdir, log_filename + '_structured.csv'), index=False)
        occ_dict = dict(log_dataframe['EventTemplate'].value_counts())
        template_df = pd.DataFrame()
        template_df['EventTemplate'] = log_dataframe['EventTemplate'].unique()
        template_df['EventId'] = template_df['EventTemplate'].map(lambda x: self.id_map[x])
        template_df['Occurrences'] = template_df['EventTemplate'].map(occ_dict)
        template_df.to_csv(os.path.join(self.outdir, log_filename + '_templates.csv'), index=False, columns=['EventId', 'EventTemplate', 'Occurrences'])

    def _preprocess_template(self, template):
        template = re.sub("<NUM>", "<*>", template)
        if template.count("<*>") > 5:
            first_start_pos = template.index("<*>")
            template = template[0:first_start_pos + 3]
        return template


def message_split(message):
    splitter_regex = re.compile('(<\*>|[^A-Za-z])')
    tokens = re.split(splitter_regex, message)
    tokens = list(filter(lambda x: x!='', tokens))
    tokens = [token for idx, token in enumerate(tokens) if token != '' and not (token == "<*>" and idx > 0 and tokens[idx-1]=="<*>")]
    # print(tokens)
    return tokens


def tree_match(match_tree, log_list):
    print("Worker {} start matching {} lines.".format(os.getpid(), len(log_list)))
    log_template_dict = {}
    for log_content in log_list:
        # if not "is false" in log_content: continue
        # Full match
        if log_content in match_tree["$NO_STAR$"]:
            log_template_dict[log_content] = (log_content, [])
            continue

        log_tokens = message_split(log_content)
        template, parameter_str = match_template(match_tree, log_tokens)
        log_template_dict[log_content] = (template if template else "NoMatch", parameter_str)
    # sys.exit()
    return log_template_dict

def match_template(match_tree, log_tokens):
    result = []
    find_template(match_tree, log_tokens, result, [])
    if result:
        result.sort(key=lambda x: (-x[1][0], x[1][1]))
        return result[0][0], result[0][2]
    return None, None

def find_template(move_tree, log_tokens, result, parameter_list):
    if len(log_tokens) == 0:
        for key, value in move_tree.items():
            if isinstance(value, tuple):
                result.append((key, value, tuple(parameter_list)))
        if "<*>" in move_tree:
            parameter_list.append("")
            move_tree = move_tree["<*>"]
            for key, value in move_tree.items():
                if isinstance(value, tuple):
                    result.append((key, value, tuple(parameter_list)))
        return
    token = log_tokens[0]

    if token in move_tree:
        find_template(move_tree[token], log_tokens[1:], result, parameter_list)

    if "<*>" in move_tree:
        if isinstance(move_tree["<*>"], dict):
            next_keys = move_tree["<*>"].keys()
            next_continue_keys = []
            for nk in next_keys:
                nv = move_tree["<*>"][nk]
                if not isinstance(nv, tuple):
                    next_continue_keys.append(nk)

            idx = 0
            # print "Now>>>", log_tokens
            # print "next>>>", next_continue_keys
            while idx < len(log_tokens):
                token = log_tokens[idx]
                # print("try", token)
                if token in next_continue_keys:
                    # print("add", "".join(log_tokens[0:idx]))
                    parameter_list.append("".join(log_tokens[0:idx]))
                    # print("End at", idx, parameter_list)
                    find_template(move_tree["<*>"], log_tokens[idx:], result, parameter_list)
                    if parameter_list:
                        parameter_list.pop()
                    # print("back", parameter_list)
                idx += 1
            if idx == len(log_tokens):
                parameter_list.append("".join(log_tokens[0:idx]))
                find_template(move_tree["<*>"], log_tokens[idx+1:], result, parameter_list)
                if parameter_list:
                    parameter_list.pop()


# print(message_split("Lustre mount FAILED : bglio46 : point /p/gb1"))
# print(message_split("Lustre mount FAILED : bglio<*> : point <*>"))
# print(message_split("generating core.<*>"))
# print(message_split("<*>."))
# print(message_split("generating core.862"))
