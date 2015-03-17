# -*- coding: utf-8 -*-

# Usage: use this as a map/reduce to gather data.
# mkdir -p /mnt/telemetry/work/cache
# cd ~/telemetry-server
# python -m mapreduce.job ./searchdata/searchmr.py    --input-filter ./searchdata/telemetry.json    --num-mappers 16    --num-reducers 16    --data-dir /mnt/telemetry/work/cache    --work-dir /mnt/telemetry/work    --output ./output.out    --bucket "telemetry-published-v2"

# Copy results to Excel, pivot table
# Copy from pivot table to a new tab and post-process from there.


import json
from collections import defaultdict, Counter
import re
from random import SystemRandom
randnum = SystemRandom()
oneOffre = re.compile('^(.+)\.')
oneOffOtherRe = re.compile('^other-')


acceptEngines = set(["Google","Yahoo","Bing", "DuckDuckGo"])

def map(k, d, v, cx):

### Basic filtering & sampling
    if randnum.randint(1,10) != 1: # Change this to adjust sampling (sampling more helps reduce go faster)
        return
    j = json.loads(v)
    try:
        locale = j["payload"]["info"]["locale"]
    except KeyError:
        try:
            locale = j["environment"]["settings"]["locale"]
        except KeyError:
            try:
                locale = j["info"]["locale"]
            except KeyError:
                return
    try:
        UITelem = j["simpleMeasurements"]["UITelemetry"]
    except KeyError:
        try:
            UITelem = j["payload"]["simpleMeasurements"]["UITelemetry"]
        except KeyError:
            return
    if (locale != 'en-US'):
        return
    
    out = ''
    
### Basic measures (Keep this in all runs)

    if not "toolbars" in UITelem:
        return
    if not "currentSearchEngine" in UITelem["toolbars"]:
        engine = u'NONE'
    else:
        engine = UITelem["toolbars"]["currentSearchEngine"]
        if engine not in acceptEngines:
            engine = u'Other'
    if (not "oneOffSearchEnabled" in UITelem["toolbars"]
        or UITelem["toolbars"]["oneOffSearchEnabled"]):
        cx.write("count\t"+engine, 1)
    else:
        cx.write("oldSearch\t"+engine, 1) # this turns out not to be significant.
        return
        
### Keep this section if you want SAPs or selections

    try:
        searchEvents = UITelem["toolbars"]["countableEvents"]["__DEFAULT__"]["search"]
    except KeyError:
        pass
    else:
        for (key, value) in searchEvents.iteritems():
        
### Search selections: keep if you want search selection data
            if isinstance(value, int):
                cx.write("totalsearches-"+key+"\t"+engine, value)
                cx.write("totalsessions-"+key+"\t"+engine, 1)
                
### Search selections (comment out if you don't care about that)
            elif (key == "selection"):
                totaluse=defaultdict(int)
                for itemvalue in value.values():
                    for (sel, events) in itemvalue.iteritems():
                        try:
                            totaluse[sel] += sum(events.values())
                        except:
                            print "events:", events
                            raise;
                for sel in totaluse:
                    if (sel == "0" and totaluse[sel] > 0):
                        cx.write("selectsessions-0\t"+engine, 1)
                        cx.write("selectsearches-0\t"+engine, totaluse[sel])
                        cx.write("selectsearch-0\t"+engine, totaluse[sel])
                    elif (int(sel) > 15 and totaluse[sel] > 0):
                        cx.write("selectsessions-1+\t"+engine, 1)
                        cx.write("selectsearches-1+\t"+engine, totaluse[sel])
                        cx.write("selectsearch-16+\t"+engine, totaluse[sel])
                    elif (totaluse[sel] > 0):
                        cx.write("selectsessions-1+\t"+engine, 1)
                        cx.write("selectsearches-1+\t"+engine, totaluse[sel])
                        cx.write("selectsearch-"+str(sel)+"\t"+engine, totaluse[sel])
### Search one-off (comment out this section if you don't care about that)
    try:
        oneOff = UITelem["toolbars"]["countableEvents"]["__DEFAULT__"]["search-oneoff"]
    except KeyError:
        pass
    else:
        for (key, value) in oneOff.iteritems():
            if key == 'other.unknown':
                engineOO = 'DEFAULT'
            else:
                match = oneOffre.match(key)
                if (match):
                    engineOO = match.group(1)
                    if (oneOffOtherRe.match(engineOO)):
                        engineOO = 'OTHER'
                else:
                    print "key", key;
            totalOOuse = 0
            for itemvalue in value.values():
                totalOOuse += sum(itemvalue.values())
            cx.write("oosearch-"+engineOO+"\t"+engine, totalOOuse)
            cx.write("oosns-"+engineOO+"\t"+engine, 1)


def reduce(k, v, cx):
    c = sum(v)
    cx.write(k.encode('utf8','xmlcharrefreplace'), c)

combine = reduce
