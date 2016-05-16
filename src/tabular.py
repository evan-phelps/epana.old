# -------------------------------------------
# package  : epana
# author   : evan.phelps@gmail.com
# created  : Sat May 14 19:00:46 EST 2016
# vim      : ts=4

import os
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
    pwd = None
    srvr_pwds = {}
    this_open = partial(open, mode='rb')
    tokens = fpath.split(':')
    if len(tokens) > 1:
        (user, server) = tokens[0].split('@')
        srvr_pwds[tokens[0]] = srvr_pwds[tokens[0]] \
            if tokens[0] in srvr_pwds \
            else getpass.getpass('password (%s): ' % tokens[0])
        this_open = partial(ssh_open, srvr=server,
                            usr=user, pwd=srvr_pwds[tokens[0]])
        fpath = tokens[1]
    with this_open(fpath) as fin:
        ufin = fin
        # TODO: Can this be changed to decrypt and yield lines in blocks
        #       so that the whole file does not need to be decrypted before
        #       starting to yield lines?
        if fpath.endswith('.gpg'):
            gpg = gnupg.GPG(homedir='~/.gnupg')
            pwd = getpass.getpass(
                'private key password: ') if pwd is None else pwd
            d = gpg.decrypt_file(fin, passphrase=pwd, always_trust=True)
            ufin = StringIO(d.data)
        yield ufin


def load_files(fnames, delims=None, **kwargs):
    df = None
    delims = len(fnames) * ['|'] if delims is None else delims
    for (fname, delim) in zip(fnames, delims):
        with fopen(fname) as ufin:
            this_df = pd.read_table(ufin, sep=delim, dtype=str, **kwargs)
            this_df['fname'] = fname
            this_df.columns = [c.replace("'", "") for c in this_df.columns]
            df = this_df if df is None else df.append(
                this_df, ignore_index=True)
    return df


def head(buf, N=10, bytes=None):
    if bytes is True:
        # return buf.read(N).split(os.linesep)
        return buf.read(N)
    else:
        return [buf.next().rstrip(os.linesep) for i in xrange(N)]


def head_gpg(fn, N=10):
    gpg = gnupg.GPG(homedir='~/.gnupg')
    pwd = getpass.getpass('private key password: ')
    return head(StringIO(gpg.decrypt_file(StringIO(head(fopen(fn), 131076,
                                                        bytes=True)),
                                          passphrase=pwd,
                                          always_trust=True).data),
                N)


def get_cols(fn, sep='|'):
    return [(i, cname) for (i, cname) in
            enumerate(head_gpg(fn, 1)[0].split(sep))]


def count_cfreq_prec(fn, char='|'):
    c = Counter()
    with fopen(fn) as ufin:
        for rec in ufin:
            c[rec.count(char)] += 1
    return c


def prof_freq(df, attgrp, agglvl=0, multi_idx=False):
    attsumm = None
    attgrp = [attgrp] if isinstance(attgrp, basestring) else attgrp
    if len(attgrp) == 1:
        agglvl = 0
        att = attgrp[0]
        attsumm = pd.DataFrame({'COUNT': df[att].value_counts()})
        attsumm.index.names = [att]
#         attsumm = attsumm.reset_index(level=att)
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


def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')
