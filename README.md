# epana

Miscellaneous file/data analysis functions.

Notes:

* In Oracle Linux 6, I had to...
** yum install libffi-devel
** pip install paramiko, even though it's in the requirements.txt for auto-installation

## Example IPython session:  get cdw ids

This was slow:

```
%load_ext sql
import pandas as pd
%config SqlMagic.autopandas=True
import getpass

user = 'ephelps'
password = getpass.getpass('ephelps@dtprd2: ')
c_dtprd2 = 'oracle://%s:%s@hssc-cdwr3-dtdb-p:1521/dtprd2'%(user, password)
%sql $c_dtprd2 select 1 from dual

%time vnums_cdw = %sql $c_dtprd2 select htb_enc_id_root, htb_enc_id_ext from cdw.visit where datasource_id = 25
vnums_cdw.to_csv('vnums_MUSC.csv')

%time patids_cdw = %sql $c_dtprd2 select mpi_systemcode, mpi_lid from cdw.patient_id_map where mpi_systemcode like 'MUSC%'
# Wall time: 4min 44s
patids_cdw.to_csv('pnums_MUSC.csv')
```

## Example IPython session:  relational counts

(22 seconds)

```
import os, sys
sys.path.insert(0, os.path.abspath("/home/ephelps/projects/dev/epana/src"))
import tabular, glob, getpass
pwd = getpass.getpass('pwd: ')
fns = sorted(glob.glob('MUSC*_3.dat.gpg'))
%time dfs = [tabular.load_files([fn], pwd=pwd, error_bad_lines=False) for fn in fns]
[(fn,len(df),len(df.columns)) for fn,df in zip(fns,dfs)]
df_dx, df_lab, df_ma, df_mo, df_mpi, df_px, df_enc, df_vit = dfs

import pandas as pd

[(fn,len(df),len(df.columns)) for fn,df in zip(fns,dfs)]
lbls = ['dx', 'lab', 'ma', 'mo', 'pat', 'proc', 'enc', 'vit']

b2x = { True: 'X', False: '' }

dfslbls = zip(*[(df, lbl) for (df, lbl) in zip(dfs, lbls) if lbl != 'pat'])

#%time vnums_musc = pd.read_csv('vnums_MUSC.csv', dtype=str, usecols=[2], names=['VISIT_ID'])
%time vnums_musc = pd.read_csv('vnums_musc_epic.dsv', dtype=str, usecols=[2,4], sep='|', names=['VISIT_ID', 'PATIENT_CLASS'])

names = list(dfslbls[1])
df_exsts_encids = tabular.count_existence_patterns(list(dfslbls[0]), names, keycol='VISIT_ID')
df_exsts_encids[names] = df_exsts_encids[names].apply(lambda x: x.map(b2x))

# or to combine cdw check for all that exist...
outer_exists = tabular.outer_existence_pattern(list(dfslbls[0]), names, 'VISIT_ID')
outer_exists['enc_in_cdw'] = outer_exists.VISIT_ID.isin(vnums_musc.VISIT_ID)
#df_exsts_encids_wCDW = tabular.freq(outer_exists, list(dfslbls[1])+['enc_in_cdw'])
#df_exsts_encids_wCDW[names+['enc_in_cdw']] = df_exsts_encids_wCDW[names+['enc_in_cdw']].apply(lambda x: x.map(b2x))

import random
s10 = lambda x: ','.join(random.sample(x, 10 if len(x)>=10 else len(x)))

vnums = pd.concat([vnums_musc.set_index('VISIT_ID', drop=False)[['VISIT_ID', 'PATIENT_CLASS']], df_enc.set_index('VISIT_ID', drop=False)[['VISIT_ID', 'PATIENT_CLASS']]])
df_exsts_encids_wCDW = outer_exists.merge(vnums, how='left', left_on='VISIT_ID', right_on='VISIT_ID').fillna('?').groupby(names+['enc_in_cdw', 'PATIENT_CLASS']).aggregate(['count', 'min', 'max', s10]).reset_index()
df_exsts_encids_wCDW[names+['enc_in_cdw']] = df_exsts_encids_wCDW[names+['enc_in_cdw']].apply(lambda x: x.map(b2x))
df_exsts_encids_wCDW.columns = names + ['enc_in_cdw', 'class', 'n_encids', 'encid_i', 'encid_f', 'encid_rnd_samples']
#outer_exists.groupby(names+['enc_in_cdw']).aggregate(['count', 'min', 'max', s10]).reset_index()
#outer_exists[(~outer_exists.enc)&(~outer_exists.enc_in_cdw)].sort(names)[names].apply(lambda x: x.map(b2x))
#outer_exists[(~outer_exists.enc)&(~outer_exists.enc_in_cdw)].sort(names)[names].apply(lambda x: x.map(b2x)).to_excel('musc_epic_incr_orphans.xlsx')

fnout = 'musc_epic_incr_rela.xlsx'
xlwrtr = pd.ExcelWriter(fnout, engine='xlsxwriter')
outer_exists[(~outer_exists.enc)&(~outer_exists.enc_in_cdw)].sort(names+['enc_in_cdw'])[names+['enc_in_cdw']].apply(lambda x: x.map(b2x)).to_excel(xlwrtr, sheet_name='all orphans', index=True)
df_exsts_encids_wCDW.to_excel(xlwrtr, sheet_name='encounter integrity', index=False)

df_mpi_pids = tabular.load_files(['MUSC_MPI_Extract_20150920_20151017_3.dat.gpg'], usecols=[0], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_enc_pids = tabular.load_files(['MUSC_Visit_Extract_20150920_20151017_3.dat.gpg'], usecols=[2], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_dx_pids = tabular.load_files(['MUSC_Diagnosis_Extract_20150920_20151017_3.dat.gpg'], usecols=[3], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_px_pids = tabular.load_files(['MUSC_Procedure_Extract_20150920_20151017_3.dat.gpg'], usecols=[3], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_vit_pids = tabular.load_files(['MUSC_Vitals_Extract_20150920_20151017_3.dat.gpg'], usecols=[5], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_mo_pids = tabular.load_files(['MUSC_MED_Order_Extract_20150920_20151017_3.dat.gpg'], usecols=[2], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_ma_pids = tabular.load_files(['MUSC_MED_Admin_Extract_20150920_20151017_3.dat.gpg'], usecols=[2], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
df_lr_pids = tabular.load_files(['MUSC_Lab_Extract_20150920_20151017_3.dat.gpg'], usecols=[2], pwd=pwd)[['PATIENT_ID']].drop_duplicates()
dfs = [df_mpi_pids, df_enc_pids, df_dx_pids, df_px_pids, df_lr_pids, df_mo_pids, df_ma_pids]

for df in dfs:
        df['PATIENT_ID_NUM'] = pd.to_numeric(df.PATIENT_ID)

df_cdw_pids = tabular.load_files(['pnums_MUSC.csv'], delims=[','], usecols=[1,2])
df_cdw_pids.columns = ['SYS', 'PATIENT_ID', 'fname']
df_cdw_pids['PATIENT_ID_NUM'] = pd.to_numeric(df_cdw_pids.PATIENT_ID)
df_cdw_pids = df_cdw_pids[df_cdw_pids.PATIENT_ID_NUM.isin(pd.concat(dfs).PATIENT_ID_NUM.unique())]
df_cdw_pids_epic = df_cdw_pids[df_cdw_pids.SYS=='MUSC_EPIC'][['PATIENT_ID', 'PATIENT_ID_NUM']]
df_cdw_pids_not_epic = df_cdw_pids[df_cdw_pids.SYS!='MUSC_EPIC'][['PATIENT_ID', 'PATIENT_ID_NUM']]
df_cdw_pids = df_cdw_pids[['PATIENT_ID', 'PATIENT_ID_NUM']]
df_cdw_pids.dropna(inplace=True)
df_cdw_pids_epic.dropna(inplace=True)
df_cdw_pids_not_epic.dropna(inplace=True)

dfs.append(df_cdw_pids_epic)
dfs.append(df_cdw_pids_not_epic)
#dfs.append(df_cdw_pids)

names = ['mpi', 'enc', 'dx', 'px', 'lr', 'mo', 'ma', 'cdw_epic', 'cdw_not_epic'] #, 'cdw_either']
df_pat_id_ptrns = tabular.outer_existence_pattern(dfs, names, 'PATIENT_ID')
df_outer_str = tabular.freq(df_pat_id_ptrns[df_pat_id_ptrns.mpi|df_pat_id_ptrns.enc|df_pat_id_ptrns.dx|df_pat_id_ptrns.px|df_pat_id_ptrns.lr|df_pat_id_ptrns.mo|df_pat_id_ptrns.ma], names) #.sort(['mpi','cdw_either', 'enc'])
tabular.print_full(df_outer_str)

df_outer_num = tabular.freq(tabular.outer_existence_pattern(dfs, names, 'PATIENT_ID_NUM'), names) #.sort(['mpi','cdw_either', 'enc'])
tabular.print_full(df_outer_num)

#df_outer_num[~(df_outer_num.mpi|df_outer_num.cdw_epic|df_outer_num.cdw_not_epic)].to_excel(xlwrtr, sheet_name='patient_id_num', index=False)
#df_outer_str[~(df_outer_str.mpi|df_outer_str.cdw_epic|df_outer_str.cdw_not_epic)].to_excel(xlwrtr, sheet_name='patient_id_str', index=False)
df_outer_num.to_excel(xlwrtr, sheet_name='patient integrity, num', index=False)
df_outer_str.to_excel(xlwrtr, sheet_name='patient integrity, str', index=False)

xlwrtr.save()

# the rest of this code block is old... before the count functions were added into the library...
patids = [df.PATIENT_ID.unique() for df in dfs]
dfo = pd.concat([pd.Series(pids, index=pids) for pids in patids], axis=1, ignore_index=True)
dfo.columns = lbls
dfo = ~dfo.isnull()
%time pnums_musc = pd.read_csv('pnums_MUSC.csv', dtype=str, usecols=[1,2], names=['SYSCD','PATIENT_ID'])
pnums_musc.set_index('PATIENT_ID')
dfo['pat_in_cdw'] = dfo.index.isin(pnums_musc.PATIENT_ID)
dfo['pat_in_cdw_loose'] = dfo.index.str.lstrip('0').isin(pnums_musc.PATIENT_ID.str.lstrip('0'))
dfo = dfo.apply(lambda x: x.map(b2x))

dfo.reset_index(inplace=True)
df_rela_patids = dfo.groupby(list(lbls) + ['pat_in_cdw', 'pat_in_cdw_loose']).count().reset_index()
df_rela_patids.columns = list(df_rela_patids.columns[0:-1]) + ['n_patids']

lbls2, encids = zip(*[(lbl, df.VISIT_ID.unique()) for (lbl,df) in zip(lbls,dfs) if lbl != 'pat'])
dfo2 = pd.concat([pd.Series(eids, index=eids) for eids in encids], axis=1, ignore_index=True)
dfo2.columns = lbls2
dfo2 = ~dfo2.isnull()
%time vnums_musc = pd.read_csv('vnums_MUSC.csv', dtype=str, usecols=[2], names=['VISIT_ID'])
dfo2['enc_in_cdw'] = dfo2.index.isin(vnums_musc.VISIT_ID)
dfo2 = dfo2.apply(lambda x: x.map(b2x))
dfo2 = pd.concat([df_enc[['VISIT_ID', 'PATIENT_CLASS']].set_index('VISIT_ID'), dfo2], axis=1).fillna('?')
dfo2.columns = ['class'] + list(dfo2.columns[1:])

dfo2.reset_index(inplace=True)
df_rela_encids = dfo2.groupby(['class'] + list(lbls2) + ['enc_in_cdw']).count().reset_index()
df_rela_encids.columns = list(df_rela_encids.columns[0:-1]) + ['n_encids']

df_rela_patids = df_rela_patids[['pat', 'enc', 'dx', 'proc', 'mo', 'ma', 'lab', 'vit', 'pat_in_cdw', 'pat_in_cdw_loose', 'n_patids']]

df_rela_encids = df_rela_encids[['class', 'enc', 'dx', 'proc', 'mo', 'ma', 'lab', 'vit', 'enc_in_cdw', 'n_encids']]

fnout = 'musc_epic_incr_rela.xlsx'
xlwrtr = pd.ExcelWriter(fnout, engine='xlsxwriter')
df_rela_patids.to_excel(xlwrtr, sheet_name='patient_id_xtab', index=False)
df_rela_encids.to_excel(xlwrtr, sheet_name='visit_id_xtab', index=False)
xlwrtr.save()
```

```
In [2]: sum(df_ma.MED_ORDER_ID.isin(df_mo.MED_ORDER_ID))
Out[2]: 855224

In [3]: len(df_ma.MED_ORDER_ID)
Out[3]: 855230

In [4]: sum(df_mo.MED_ORDER_ID.isin(df_ma.MED_ORDER_ID))
Out[4]: 199011

In [5]: len(df_mo.MED_ORDER_ID)
Out[5]: 356467

In [9]: df_ma[~df_ma.MED_ORDER_ID.isin(df_mo.MED_ORDER_ID)][['PATIENT_ID', 'VISIT_ID', 'MED_ORDER_ID', 'ADMINISTRATIONNUMBER']]
Out[9]: 
       PATIENT_ID    VISIT_ID MED_ORDER_ID ADMINISTRATIONNUMBER
85034   001554336  1003993244     47138153           47138153-6
134560  001554336  1003993244     47138150           47138150-2
337946  001554336  1003993244     47138153           47138153-7
516765  001554336  1003993244     44094972           44094972-2
556834  001554336  1003993244     44094972           44094972-4
811784  001554336  1003993244     44094972           44094972-3

In [17]: len(df_mo)
Out[17]: 356467

In [18]: sum(df_mo.VISIT_ID.isin(df_enc.VISIT_ID))
Out[18]: 356467
```

## Heights
```
import os, sys
sys.path.insert(0, os.path.abspath("/home/ephelps/projects/dev/epana/src"))
import tabular, glob, getpass
%matplotlib
import matplotlib as mpl
pwd = getpass.getpass('pwd: ')

df_new = tabular.load_files(['MUSC_VITALS_Extract_20150920_20151017.dat.gpg'], pwd)
df_old = tabular.load_files(['phelpse@hssc-hb0-s:/home/phelpse/projects/musc/MUSC_Vitals_EPIC_20140701_20150919.dat.gpg'], pwd)
#heights = df_new[df_new.OBSERVATION_NAME=='HEIGHT']['OBSERVATION_VALUE'].convert_objects(convert_numeric=True)


df_new[df_new.OBSERVATION_NAME=='HEIGHT']['OBSERVATION_VALUE'].convert_objects(convert_numeric=True).hist(bins=[-0.5+i for i in range(100)], normed=True)
(df_old[df_old.OBSERVATION_TYPE_DESC=='Height']['OBSERVATION_VALUE'].convert_objects(convert_numeric=True)*2.54).hist(bins=100, normed=True, alpha=0.5)
```

## Relational 2
```
df_rel = tabular.count_outer_relations(list(dfslbls[0]), names, keycol='VISIT_ID')
df_rel[df_rel.columns[:-1]] = df_rel[df_rel.columns[:-1]]>0
df_rel.head()
df_rel.groupby(list(df_rel.columns[:-1])).aggregate(['min', 'count']).reset_index()
```

