import json, re
from collections import Counter
rows = json.load(open('tickets/ti_1053_elevenlabs_3p_segments/outputs/candidate_pool.json'))

# ── ElevenLabs = NICHE B2B: AI voice/audio (TTS, voice cloning, dubbing, AI audio).
# Real ICP: AI/ML devs & engineers (API), content creators, podcast/audiobook/voiceover,
# video/film/media production, animation, game audio, creative marketers, edtech narration,
# enterprise conversational-AI/CX. NOT "general businesses". Broad B2B = dilution = weak incrementality.

# theme -> (tier, niche weight). Tier1 = core niche, Tier2 = adjacent-creative, Tier3 = broad context.
THEME_DEFS=[
 ('AI & Machine Learning',1,['artificial intelligence','machine learning']),
 ('Developers & Software Engineering',1,['software develop','software design','software engineer','web develop','programmer','developer','software publish','computer programming','computer systems']),
 ('Audio · Podcast · Audiobook · Voiceover',1,['voiceover','voice over','voice talent','voice actor','podcast','audiobook','narration','audio produc','sound design']),
 ('Content Creation & Media Production',1,['content creat','film/video production','motion picture','video produc','media produc','multimedia','video edit','filmmak','animation','creator']),
 ('Gaming & Game Dev',1,['game develop','game design','gaming','video game']),
 ('Marketing & Advertising (creative)',2,['marketing','advertis','social media','digital marketing']),
 ('Design & Creative',2,['graphic design','designer','ux','creative services']),
 ('Education & EdTech',2,['e-learning','edtech','education technology','teacher - technology','academic > teacher']),
 ('Publishing & Media',2,['publishing','newspapers','news services','broadcasting']),
 ('IT & Software Industry (broad)',3,['software','information technology','technology','computer','saas','cloud','semiconductor','consumer electronics']),
 ('Telecom & Customer Service (CX/IVR)',3,['telecommunication','customer service','call center','contact center']),
 ('General B2B / Decision-Makers (broad)',3,['decision maker','executive','business owner','startup','entrepreneur','enterprise','b2b','professional']),
]
NEG=['physician','nurse','dental','medical','healthcare','hospital','pharma','patient','surgery','clinic',
 'gardener','landscaper','agricultur','farm','mining','rubber','missile','mobile home','funeral','plumb',
 'hvac','roofing','construction','manufactur','assembly','fabricat','metal stamp','propulsion','restaurant',
 'grocery','real estate','insurance agent','mortgage','veterinar','dentist','truck','freight','oil & gas',
 'janitor','pest control','lead pencil','crayon','textile','aircraft','automotive','furniture']
NEG_CONTENT=['tvv >','title:','wild kratts','sitcom','anime','soccer','mls ','nfl','nba','mlb',
 'reality tv','daytime','sports fans','ott subscriber','video streaming']
INMARKET=['in-market','in market','intent','shopper','purchase','buyers','ready to buy','propensity']
DEMO=['household income','hhi','age range','net worth','homeowner','presence of children','ethnicity','gender >']

def classify(path):
    p=' '+path.lower()+' '
    for name,tier,ks in THEME_DEFS:
        if any(k in p for k in ks): return name,tier
    return None,9

def score(path):
    p=' '+path.lower()+' '; s=0
    if any(k in p for k in NEG): s-=8
    if any(k in p for k in NEG_CONTENT): s-=8
    # niche-core keyword weight
    core=['artificial intelligence','machine learning','voiceover','voice talent','voice actor','podcast',
          'audiobook','narration','content creat','film/video production','motion picture','video produc',
          'media produc','multimedia','software develop','web develop','programmer','game develop','animation']
    for k in core:
        if k in p: s+=4
    mid=['software','information technology','marketing','advertis','graphic design','designer','gaming',
         'publishing','telecommunication','customer service','e-learning','education','creative','technology']
    for k in mid:
        if k in p: s+=2
    # penalties: mixed-bundle (diluted) + pure broad-firmographic
    if 'industry (multiple categories)' in p or p.count(';')>=2: s-=2   # bundled/diluted
    if any(k in p for k in INMARKET): s-=2
    if any(k in p for k in DEMO): s-=2
    seg_type='in-market' if any(k in p for k in INMARKET) else ('interest/affinity' if ('interest' in p or 'enthusiast' in p or 'likely to be' in p) else 'firmographic/role')
    return s, seg_type

scored=[]
for r in rows:
    path=r.get('path_from_root') or r.get('name') or ''
    theme,tier=classify(path)
    if theme is None: continue
    s,typ=score(path)
    if s<4: continue
    scored.append({'cat':r['cat'],'path':path,'leaf':r.get('name',''),'theme':theme,'tier':tier,
                   'name_score':s,'seg_type':typ})

# dedup by (theme, normalized leaf) keep best
best={}
for x in scored:
    key=(x['theme'], re.sub(r'\s+',' ',x['leaf'].lower()).strip()[:55])
    if key not in best or x['name_score']>best[key]['name_score']: best[key]=x
dd=sorted(best.values(), key=lambda x:(x['tier'],-x['name_score'],x['theme']))
json.dump(dd, open('tickets/ti_1053_elevenlabs_3p_segments/outputs/scored_deduped.json','w'))
print(f"deduped relevant: {len(dd)}")
print("tier counts:", dict(sorted(Counter(x['tier'] for x in dd).items())))
print("theme counts:")
for t,n in Counter((x['tier'],x['theme']) for x in dd).most_common(): print(f"  T{t[0]} {n:>3}  {t[1]}")
print("\n=== TIER 1 (core niche) ===")
for x in [d for d in dd if d['tier']==1][:30]:
    print(f"  {x['name_score']:>3} [{x['theme'][:34]:34}] {x['path'][:70]}")

# ── FINAL CURATION (niche balance + exhibition/retail demotion + dedup of motion-picture variants) ──
EXHIB=['theater','theatre','drive-in','suncoast','retail','rental','allied to motion','video store','exhibition']
dd2=[]
for x in dd:
    p=x['path'].lower()
    if any(k in p for k in EXHIB):   # exhibition/retail = consumers, not creators
        continue
    dd2.append(x)
# collapse near-duplicate "motion picture / video production" firmographic variants to top 3
mpv=[x for x in dd2 if 'motion picture' in x['path'].lower() or 'video produc' in x['path'].lower()]
mpv_keep=set(id(x) for x in sorted(mpv,key=lambda z:-z['name_score'])[:3])
final=[x for x in dd2 if not(('motion picture' in x['path'].lower() or 'video produc' in x['path'].lower()) and id(x) not in mpv_keep)]
final=sorted(final,key=lambda x:(x['tier'],-x['name_score'],x['theme']))
json.dump(final, open('tickets/ti_1053_elevenlabs_3p_segments/outputs/final_ranked.json','w'))
print("\n\n##### FINAL CURATED LIST #####  count:",len(final))
print("CATS:",",".join(str(x['cat']) for x in final))
print("\nby theme:")
for t,n in Counter(x['theme'] for x in final).most_common(): print(f"  {n:>2}  {t}")
