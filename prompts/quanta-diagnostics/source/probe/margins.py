import sqlite3, glob, json, sys
import pandas as pd, numpy as np
MP_eV=2.4353e27
rows=[]
for f in sorted(glob.glob(sys.argv[1]+"-shard*.db")):
    c=sqlite3.connect(f"file:{f}?mode=ro",uri=True)
    q="""select h.serial, m.value_eV/%g as M, b.value as beta, a.value as alpha, h.failure,
       h.worst_birth_margin, h.worst_reflection_margin, h.worst_tracker_margin, h.worst_tracker_margin_N,
       h.birth_failing_share, h.reflection_failing_share, h.tracker_failing_share, h.unassessed_share,
       h.injections, h.rho_chi_ratio_today, h.parking_N, h.extra_data
       from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
       join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
       join alpha_value a on h.alpha_serial=a.serial""" % MP_eV
    rows+= c.execute(q).fetchall()
cols="serial M beta alpha failure wb wr wt wtN bfs rfs tfs us inj rchi parkN extra".split()
d=pd.DataFrame(rows,columns=cols)
d=d[(d.alpha>0)&(d.failure==0)]
def ex(e,k):
    if e is None: return None
    m=json.loads(e).get("margins"); return None if m is None else m.get(k)
d["bws"]=d.extra.map(lambda e: ex(e,"below_weight_share"))
d["ftf"]=d.extra.map(lambda e: ex(e,"first_tracker_failure"))
pd.set_option("display.width",250)
g=d.groupby("M")
print(g.agg(n=("serial","size"),wb_min=("wb","min"),wr_min=("wr","min"),wt_min=("wt","min"),wt_med=("wt","median"),
  bfs_max=("bfs","max"),rfs_max=("rfs","max"),tfs_max=("tfs","max"),
  n_tfail=("tfs",lambda s:(s>0).sum()),us_max=("us","max"),bws_max=("bws","max"),rchi_max=("rchi","max")))
t=d[d.tfs>0].sort_values("tfs",ascending=False)
print(t[["M","beta","alpha","wt","wtN","tfs","us","rchi","parkN"]].head(15))
print(t.ftf.head(3).tolist())
d.drop(columns="extra").to_pickle(sys.argv[2])
