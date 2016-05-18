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
fns = glob.glob('MUSC*_20150920_*.gpg')
%time dfs = [tabular.load_files([fn], pwd=pwd) for fn in fns]
[(fn,len(df),len(df.columns)) for fn,df in zip(fns,dfs)]
df_enc = dfs[1]
df_mpi, df_dx, df_px, df_lab, df_vit, df_mo, df_ma = dfs[2], dfs[6], dfs[3], dfs[7], dfs[5], dfs[0], dfs[4]

import pandas as pd

[(fn,len(df),len(df.columns)) for fn,df in zip(fns,dfs)]
lbls = ['mo', 'enc', 'pat', 'proc', 'ma', 'vit', 'dx', 'lab']

b2x = { True: 'X', False: '' }

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
