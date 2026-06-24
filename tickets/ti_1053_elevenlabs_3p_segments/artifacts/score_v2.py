import json, re
from collections import Counter
rows = json.load(open('tickets/ti_1053_elevenlabs_3p_segments/outputs/candidate_pool.json'))

# What ElevenLabs SELLS: AI voice/audio GENERATION (TTS, voice clone, dubbing, voice agents, audio API).
# BUYERS = people who PRODUCE audio/build voice features, NOT audio consumers.
def buyer_signal(p):
    if re.search(r'\b(b2b|sic|naics|industry|industries|occupation|title / profession|department|business leads|hiring|job seekers)\b', p):
        return 'buyer-firmographic'
    if any(k in p for k in ['developer','programmer','software engineer','web develop','designer','analyst','creative director','marketing professional']):
        return 'buyer-role'
    if any(k in p for k in ['machine learning','artificial intelligence',' ai ','computers >','technolog']) and ('interest' in p or 'enthusiast' in p):
        return 'technical-interest'
    if any(k in p for k in ['entertainment','likely to be','streaming media','reading audiobooks','listeners','arts & entertainment']) or p.strip().startswith(' us >') or 'interests >' in p:
        return 'consumer-affinity'   # audio/content CONSUMERS, not buyers
    return 'other'

# niche themes (core=1, adjacent=2, broad=3)
THEME=[('AI & Machine Learning',1,['artificial intelligence','machine learning']),
 ('Developers & Software Engineering',1,['software develop','software design','software engineer','web develop','programmer','developer','software publish','computer programming','computer systems']),
 ('Audio/Voice Production',1,['voiceover','voice talent','voice actor','recording studio','sound recording','audio produc','sound design','music production']),
 ('Film/Video/Media Production',1,['film/video production','motion picture','video produc','media produc','multimedia','animation','post production','broadcast']),
 ('Content/Podcast/Audiobook Production',1,['podcast production','audiobook publish','content creat','publishing']),
 ('Gaming & Game Dev',1,['game develop','game design','video game']),
 ('Marketing & Advertising (creative)',2,['advertising','marketing','digital marketing','creative agency']),
 ('Design & Creative',2,['graphic design','designer','creative services']),
 ('Education & EdTech',2,['e-learning','edtech','teacher - technology','education technology']),
 ('IT & Software Industry (broad)',3,['software','information technology','technology','computer','saas','cloud','semiconductor']),
 ('Telecom/Customer Service (CX/IVR)',3,['telecommunication','customer service','call center','contact center']),
]
NEG=['physician','nurse','dental','medical','healthcare','hospital','pharma','patient','clinic','gardener',
 'landscaper','agricultur','farm','mining','rubber','missile','mobile home','funeral','plumb','hvac','roofing',
 'construction','manufactur','assembly','fabricat','metal stamp','propulsion','restaurant','grocery','real estate',
 'insurance agent','mortgage','veterinar','dentist','truck','freight','oil & gas','janitor','pest control','textile',
 'aircraft','automotive','furniture','theater','theatre','drive-in','suncoast','video store','sports fans','soccer',
 'nfl','nba','mlb','anime','sitcom','wild kratts','tvv >','title:']

def classify(p):
    for n,t,ks in THEME:
        if any(k in p for k in ks): return n,t
    return None,9

def score(path):
    p=' '+path.lower()+' '; s=0
    if any(k in p for k in NEG): return None
    bs=buyer_signal(p)
    # relevance from niche keywords
    core=['artificial intelligence','machine learning','voiceover','voice talent','podcast','audiobook',
          'content creat','film/video production','motion picture','video produc','media produc','multimedia',
          'software develop','web develop','programmer','game develop','animation','recording','dubbing']
    s+=4*sum(k in p for k in core)
    mid=['software','information technology','marketing','advertis','graphic design','designer','gaming',
         'publishing','telecommunication','customer service','e-learning','education','creative','technology']
    s+=2*sum(k in p for k in mid)
    # BUYER vs CONSUMER: penalize consumer-affinity hard (they buy audio CONTENT, not audio TOOLS)
    incr=0
    if bs=='consumer-affinity': incr-=6
    if bs=='other': incr-=2
    if 'industry (multiple categories)' in p or p.count(';')>=2: incr-=2   # diluted bundle
    if any(k in p for k in ['in-market','in market','shopper','purchase','propensity']): incr-=2
    if any(k in p for k in ['household income','hhi','age range','net worth','homeowner']): incr-=3
    return s, incr, bs

out=[]
for r in rows:
    path=r.get('path_from_root') or r.get('name') or ''
    sc=score(path)
    if sc is None: continue
    rel,incr,bs=sc
    theme,tier=classify(' '+path.lower())
    if theme is None: continue
    comp=rel+incr
    if comp<4: continue
    out.append({'cat':r['cat'],'path':path,'leaf':r.get('name',''),'theme':theme,'tier':tier,
                'buyer_signal':bs,'relevance':rel,'incr':incr,'name_score':comp})
# dedup
best={}
for x in out:
    k=(x['theme'], re.sub(r'\s+',' ',x['leaf'].lower()).strip()[:55])
    if k not in best or x['name_score']>best[k]['name_score']: best[k]=x
dd=sorted(best.values(), key=lambda x:(x['tier'],-x['name_score']))
json.dump(dd, open('tickets/ti_1053_elevenlabs_3p_segments/outputs/scored_v2.json','w'))
print("v2 candidates:",len(dd))
print("buyer_signal:",dict(Counter(x['buyer_signal'] for x in dd)))
print("tier:",dict(sorted(Counter(x['tier'] for x in dd).items())))
print("\nTOP 25:")
for x in dd[:25]: print(f"  {x['name_score']:>2} {x['buyer_signal'][:18]:18} [{x['theme'][:28]:28}] {x['path'][:55]}")
print("\nCATS:",",".join(str(x['cat']) for x in dd[:45]))
