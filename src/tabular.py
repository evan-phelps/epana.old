# -------------------------------------------
# package  : epana
# author   : evan.phelps@gmail.com
# created  : Sat May 14 19:00:46 EST 2016
# vim      : ts=4

import os
# import re
from functools import partial
from StringIO import StringIO
import pandas as pd
# import numpy as np
# from glob import glob
import getpass
import paramiko
from contextlib import contextmanager
import gnupg
from collections import Counter
import csv


# ############################################################# #
# ################## file handling functions ################## #
# ############################################################# #
# These should probably be moved to another file ############## #


@contextmanager
def ssh_open(fpaths, srvr, usr, pwd,
             known_hosts=os.environ['HOME'] + '/.ssh/known_hosts'):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys(known_hosts)
    ssh.connect(srvr, username=usr, password=pwd)
    try:
        ftp = ssh.open_sftp()
        fpaths = [fpaths] if isinstance(fpaths, basestring) else fpaths
        for fpath in fpaths:
            yield ftp.open(fpath, 'rb', bufsize=1048576)
    finally:
        ssh.close()


@contextmanager
def fopen(fpath):
    srvr_pwds = {}
    this_open = partial(open, mode='rb')
    tokens = fpath.split(':')
    if len(tokens) > 1:
        user_server = tokens[0].split('@')
        (user, server) = (None, None)
        if len(user_server) == 2:
            (user, server) = user_server
        else:
            (user, server) = getpass.getuser(), user_server[0]
        srvr_pwds[tokens[0]] = srvr_pwds[tokens[0]] \
            if tokens[0] in srvr_pwds \
            else getpass.getpass('password (%s): ' % tokens[0])
        this_open = partial(ssh_open, srvr=server,
                            usr=user, pwd=srvr_pwds[tokens[0]])
        fpath = tokens[1]
    with this_open(fpath) as fin:
        yield fin


def decrypt(fin, pwd=None, ostream=False):
    # Some systems might need binary=/usr/bin/gpg2 if multiple versions
    # are installed.  Consider adding parameter to specify.
    gpg = gnupg.GPG(homedir='~/.gnupg')
    pwd = getpass.getpass(
        'private key password: ') if pwd is None else pwd
    d = gpg.decrypt_file(fin, passphrase=pwd, always_trust=True)
    if ostream is True:
        return StringIO(d.data)
    else:
        return d.data.rstrip(os.linesep).split(os.linesep)


def head(fname, N=10, bytes=None):
    if fname.endswith('.gpg'):
        s = None
        with fopen(fname) as fin:
            s = fin.read(131076)
        pwd = getpass.getpass('private key password: ')
        return decrypt(StringIO(s), pwd)[0:N]
    elif bytes is True:
        # return buf.read(N).split(os.linesep)
        with fopen(fname) as buf:
            return buf.read(N)
    else:
        with fopen(fname) as buf:
            return [buf.next().rstrip(os.linesep) for i in xrange(N)]


def get_cols(fn, sep='|'):
    return [(i, cname) for (i, cname) in
            enumerate(head(fn, 1)[0].split(sep))]


# TODO: Incorporate this regex version into the active count_cfreq_prec
# def count_cfreq_prec(fn, patterns):
#     cntrs = {ptrn: Counter() for ptrn in patterns}
#     ptrns_compiled = {ptrn: re.compile(ptrn) for ptrn in patterns}
#     with fopen(fn) as fin:
#         ufin = decrypt(fin) if fn.endswith('.gpg') else fin
#         for rec in ufin:
#             for ptrn, c in cntrs.items():
#                 c[len(ptrns_compiled[ptrn].findall(rec))] += 1
#     return cntrs


def count_cfreq_prec(fn, patterns):
    cntrs = {ch: Counter() for ch in patterns}
    with fopen(fn) as fin:
        ufin = decrypt(fin) if fn.endswith('.gpg') else fin
        for rec in ufin:
            for ch, c in cntrs.items():
                c[rec.count(ch)] += 1
    return cntrs


# ############################################################# #
# ################### tabular data functions ################## #
# ############################################################# #

# **kwargs):
def load_files(fnames, pwd=None, delims=None, dtype=str,
               quotechar="'", escapechar="'", quoting=csv.QUOTE_NONE,
               usecols=None, error_bad_lines=True):
    df = None
    delims = len(fnames) * ['|'] if delims is None else delims
    for (fname, delim) in zip(fnames, delims):
        with fopen(fname) as fin:
            ufin = fin
            if fname.endswith('.gpg'):
                pwd = getpass.getpass(
                    'private key password: ') if pwd is None else pwd
                ufin = decrypt(fin, pwd, ostream=True)
            this_df = pd.read_table(ufin, sep=delim, dtype=dtype,
                                    quotechar=quotechar, quoting=quoting,
                                    usecols=usecols, encoding='utf-8',
                                    error_bad_lines=error_bad_lines)
            this_df['fname'] = fname
            this_df.columns = [c.replace("'", "") for c in this_df.columns]
            df = this_df if df is None else df.append(
                this_df, ignore_index=True)
    return df


def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')


def freq(df, attgrp, agglvl=0, multi_idx=False):
    attsumm = None
    attgrp = [attgrp] if isinstance(attgrp, basestring) else attgrp
    if len(attgrp) == 1:
        agglvl = 0
        att = attgrp[0]
        attsumm = pd.DataFrame({'COUNT': df[att].value_counts()})
        attsumm.index.names = [att]
    else:
        attsumm = df[attgrp].groupby(attgrp).agg(lambda x: len(x))
        attsumm = attsumm.reset_index(name='COUNT')
    attsumm = attsumm.sort('COUNT', ascending=False)
    if agglvl > 0:
        attsumm = attsumm.sort(attgrp[0:agglvl])
        attsumm['PERC'] = attsumm.groupby(
            attgrp[0:agglvl]).COUNT.apply(lambda x: 100 * x / sum(x))
        attsumm['CUMPERC'] = attsumm.groupby(attgrp[0:agglvl]).PERC.cumsum()
    else:
        attsumm['PERC'] = 100 * attsumm.COUNT / sum(attsumm.COUNT)
        attsumm['CUMPERC'] = attsumm.PERC.cumsum()
    if multi_idx:
        attsumm.set_index(attgrp, inplace=True)
    return attsumm


def gen_code_freqs(df_in, cols, fnout):
    xlwrtr = pd.ExcelWriter(fnout, engine='xlsxwriter')

    for col in cols:
        df = freq(df_in, col)
        tabname = col if isinstance(col, basestring) else '-'.join(col)[0:31]
        df.to_excel(xlwrtr, sheet_name=tabname, index=True)
    #     xlwrtr.sheets[cname].set_column('D:E', None, format_perc)
    xlwrtr.save()


def count_outer_relations(dfs, names, keycol):
    kcol = [keycol] if isinstance(keycol, basestring) else keycol
    dfs_reduced = [df[kcol].groupby(kcol)[kcol[0]].count()
                   for df in dfs]
    outer_count = pd.concat(dfs_reduced, axis=1, ignore_index=True).fillna(0)
    outer_count.columns = names
    outer_count[keycol] = outer_count.index.values
    return outer_count


def count_relational_patterns(dfs, names, keycol):
    outer_count = count_outer_relations(dfs, names, keycol)
    # outer_count.reset_index(inplace=True)
    patterns = outer_count.groupby(names).count().reset_index()
    patterns.columns = list(patterns.columns[:-1]) + ['COUNT']
    return patterns.astype(int)


def count_existence_patterns(dfs, names, keycol):
    rel_counts = count_relational_patterns(dfs, names, keycol)
    rel_counts[names] = rel_counts[names] > 0
    patterns = rel_counts.groupby(names)['COUNT'].sum().reset_index()
    return patterns
