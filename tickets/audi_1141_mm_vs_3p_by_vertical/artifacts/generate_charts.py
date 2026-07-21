#!/usr/bin/env python3
"""AUDI-1141 charts. Tufte: max data-ink, direct labels, one accent, finding-as-title.
IVR shown as %, median advertiser. Buckets: MM (gated)/MM (no gate)/MM restricted/3P."""
import pandas as pd, numpy as np
import matplotlib.pyplot as plt

plt.rcParams.update({
    "font.family":["Helvetica Neue","Helvetica","Arial","DejaVu Sans"],
    "figure.facecolor":"#FAFAFA","axes.facecolor":"#FAFAFA","savefig.facecolor":"#FAFAFA",
    "axes.spines.top":False,"axes.spines.right":False,"axes.grid":False,
    "axes.edgecolor":"#BBBBBB","xtick.color":"#555555","ytick.color":"#555555",
    "text.color":"#222222","axes.labelcolor":"#333333","figure.dpi":200})
MM="#1a6e6a"; MMNG="#8fb4b2"; MMR="#c8a45c"; TP="#9a9a9a"; RED="#c0392b"
OUT="tickets/audi_1141_mm_vs_3p_by_vertical/outputs/"; ART="tickets/audi_1141_mm_vs_3p_by_vertical/artifacts/"
BO=["MM (gated)","MM (no gate)","MM restricted","3P"]
ov=pd.read_csv(OUT+"audi_1141_scorecard_overall.csv").set_index("bucket_detail").loc[BO]
bv=pd.read_csv(OUT+"audi_1141_scorecard_by_vertical.csv")

def title(ax,t,sub):
    ax.text(0,1.11,t,transform=ax.transAxes,fontsize=13,fontweight="bold",va="bottom")
    ax.text(0,1.035,sub,transform=ax.transAxes,fontsize=8.5,color="#777777",va="bottom")

# ---- Chart 1: overall IVR + CPV by group ----
fig,(a1,a2)=plt.subplots(1,2,figsize=(12,5.2)); cols=[MM,MMNG,MMR,TP]
ivr=(ov.IVR_med*100).values
a1.bar(BO,ivr,color=cols,width=.66)
for x,v in zip(range(4),ivr): a1.text(x,v+.01,f"{v:.2f}%",ha="center",fontsize=11,fontweight="bold")
a1.set_ylim(0,ivr.max()*1.2); a1.set_ylabel("Visit rate (visits / impressions)"); a1.set_yticks([])
title(a1,"Gated MNTN Matched drives ~6x the visit rate of 3P","Median advertiser, trailing 6 months, S1 prospecting")
cpv=ov.CPV_med.values
a2.bar(BO,cpv,color=cols,width=.66)
for x,v in zip(range(4),cpv): a2.text(x,v+.6,f"${v:.0f}",ha="center",fontsize=11,fontweight="bold")
a2.set_ylim(0,cpv.max()*1.2); a2.set_ylabel("Cost per visit ($)"); a2.set_yticks([])
title(a2,"...and at about one quarter the cost per visit","Median advertiser, lower is better")
for a in (a1,a2): a.tick_params(labelsize=9)
fig.text(.5,.005,"Removing the intent gate or narrowing the audience erodes most of the edge.",
         ha="center",fontsize=9,style="italic",color=RED)
fig.tight_layout(rect=[0,.03,1,1]); fig.savefig(ART+"audi_1141_chart_overall.png",bbox_inches="tight"); plt.close(fig)

# ---- MM(gated) vs 3P by vertical ----
def by_vert(metric,fname,tt,sub,fmt,lower_better=False,as_pct=False):
    m=bv.pivot(index="sales_vertical",columns="bucket_detail",values=metric)[["MM (gated)","3P"]].dropna()
    m=m[m.index!="Other / Unmapped"]
    if as_pct: m=m*100
    m=m.sort_values("MM (gated)",ascending=True)
    fig,ax=plt.subplots(figsize=(10.5,6)); y=np.arange(len(m)); h=.38
    ax.barh(y+h/2,m["MM (gated)"],height=h,color=MM,label="MM (gated)")
    ax.barh(y-h/2,m["3P"],height=h,color=TP,label="3P")
    xmax=m[["MM (gated)","3P"]].max().max()
    for i,(mm,tp) in enumerate(zip(m["MM (gated)"],m["3P"])):
        ax.text(mm+xmax*.01,i+h/2,fmt(mm),va="center",fontsize=9,fontweight="bold",color=MM)
        ax.text(tp+xmax*.01,i-h/2,fmt(tp),va="center",fontsize=9,color="#666666")
    ax.set_yticks(y); ax.set_yticklabels(m.index,fontsize=10); ax.set_xticks([]); ax.set_xlim(0,xmax*1.16)
    title(ax,tt,sub); ax.legend(loc="lower right",frameon=False,fontsize=9.5)
    fig.tight_layout(); fig.savefig(ART+fname,bbox_inches="tight"); plt.close(fig)

by_vert("IVR_med","audi_1141_chart_ivr_by_vertical.png",
        "MNTN Matched wins visit rate in every vertical",
        "Median advertiser, visits / impressions, trailing 6 months prospecting",
        lambda v:f"{v:.2f}%",as_pct=True)
by_vert("CPV_med","audi_1141_chart_cpv_by_vertical.png",
        "MNTN Matched costs less per visit in every vertical",
        "Median advertiser, $ per visit (lower is better), trailing 6 months prospecting",
        lambda v:f"${v:.0f}",lower_better=True)
print("charts written")
