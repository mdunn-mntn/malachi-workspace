#!/usr/bin/env python3
"""AUDI-1141 charts — Tufte: max data-ink, direct labels, one accent, finding-as-title.
Reads outputs/*.csv. MM(gated)=hero teal, 3P=context gray, MM(no gate)=muted."""
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm

plt.rcParams.update({
    "font.family": ["Helvetica Neue","Helvetica","Arial","DejaVu Sans"],
    "figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA",
    "axes.spines.top":False,"axes.spines.right":False,"axes.grid":False,
    "axes.edgecolor":"#BBBBBB","xtick.color":"#555555","ytick.color":"#555555",
    "text.color":"#222222","axes.labelcolor":"#333333","figure.dpi":200})
MM="#1a6e6a"; MMNG="#8fb4b2"; TP="#9a9a9a"; MIX="#c8a45c"; RED="#c0392b"
OUT="tickets/audi_1141_mm_vs_3p_by_vertical/outputs/"; ART="tickets/audi_1141_mm_vs_3p_by_vertical/artifacts/"

ov = pd.read_csv(OUT+"audi_1141_scorecard_overall.csv").set_index("bucket_detail")
bv = pd.read_csv(OUT+"audi_1141_scorecard_by_vertical.csv")
BO=["MM (gated)","MM (no gate)","Mixed","3P"]; ov=ov.loc[BO]

def title(ax,t,sub):
    ax.text(0,1.11,t,transform=ax.transAxes,fontsize=13,fontweight="bold",va="bottom")
    ax.text(0,1.035,sub,transform=ax.transAxes,fontsize=8.5,color="#777777",va="bottom")

# ---- Chart 1: overall — VR + CPV by bucket + the gate story ----
fig,(a1,a2)=plt.subplots(1,2,figsize=(12,5.2)); cols=[MM,MMNG,MIX,TP]
vr=ov.VR_med.values; b=a1.bar(BO,vr,color=cols,width=.66)
for x,v in zip(range(4),vr): a1.text(x,v+.06,f"{v:.1f}",ha="center",fontsize=11,fontweight="bold")
a1.set_ylim(0,vr.max()*1.2); a1.set_ylabel("Visits per 1,000 impressions"); a1.set_yticks([])
title(a1,"Gated MNTN Matched drives ~4x the visit rate of 3P","Median advertiser · trailing 6mo · S1 prospecting")
cpv=ov.CPV_med.values; a2.bar(BO,cpv,color=cols,width=.66)
for x,v in zip(range(4),cpv): a2.text(x,v+.4,f"${v:.0f}",ha="center",fontsize=11,fontweight="bold")
a2.set_ylim(0,cpv.max()*1.2); a2.set_ylabel("Cost per visit ($)"); a2.set_yticks([])
title(a2,"...and at ~1/3 the cost per visit","Median advertiser · lower is better")
for a in (a1,a2): a.tick_params(labelsize=9.5)
fig.text(.5,.005,"Un-gated MM (no score threshold) collapses toward 3P — the intent gate is what creates MM's value.",
         ha="center",fontsize=9,style="italic",color=RED)
fig.tight_layout(rect=[0,.03,1,1]); fig.savefig(ART+"audi_1141_chart_overall.png",bbox_inches="tight"); plt.close(fig)

# ---- helper: MM(gated) vs 3P by vertical, sorted by MM advantage ----
def by_vert(metric,fname,tt,sub,fmt,lower_better=False):
    m=bv.pivot(index="sales_vertical",columns="bucket_detail",values=metric)
    m=m[["MM (gated)","3P"]].dropna()
    m=m[m.index!="Other / Unmapped"]
    m["adv"]=(m["3P"]/m["MM (gated)"]) if lower_better else (m["MM (gated)"]/m["3P"])
    m=m.sort_values("MM (gated)",ascending=True)
    fig,ax=plt.subplots(figsize=(10.5,6)); y=np.arange(len(m)); h=.38
    ax.barh(y+h/2,m["MM (gated)"],height=h,color=MM,label="MM (gated)")
    ax.barh(y-h/2,m["3P"],height=h,color=TP,label="3P")
    for i,(mm,tp) in enumerate(zip(m["MM (gated)"],m["3P"])):
        ax.text(mm+m["MM (gated)"].max()*.01,i+h/2,fmt(mm),va="center",fontsize=9,fontweight="bold",color=MM)
        ax.text(tp+m["MM (gated)"].max()*.01,i-h/2,fmt(tp),va="center",fontsize=9,color="#666666")
    ax.set_yticks(y); ax.set_yticklabels(m.index,fontsize=10); ax.set_xticks([])
    ax.set_xlim(0,m[["MM (gated)","3P"]].max().max()*1.15)
    title(ax,tt,sub)
    ax.legend(loc="lower right",frameon=False,fontsize=9.5)
    fig.tight_layout(); fig.savefig(ART+fname,bbox_inches="tight"); plt.close(fig)

by_vert("VR_med","audi_1141_chart_vr_by_vertical.png",
        "MNTN Matched wins visit rate in every vertical",
        "Median advertiser · visits per 1,000 impressions · trailing 6mo prospecting",
        lambda v:f"{v:.1f}")
by_vert("CPV_med","audi_1141_chart_cpv_by_vertical.png",
        "MNTN Matched costs less per visit in every vertical",
        "Median advertiser · $ per visit (lower is better) · trailing 6mo prospecting",
        lambda v:f"${v:.0f}",lower_better=True)
print("charts written to",ART)
