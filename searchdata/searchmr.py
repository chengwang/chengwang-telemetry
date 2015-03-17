# Same as the osdistribution.py example in jydoop
# -*- coding: utf-8 -*-
import json
from collections import defaultdict, Counter
import re
from random import SystemRandom
randnum = SystemRandom()

acceptEngines = set(["Google","Yahoo","Bing", "DuckDuckGo"])

def map(k, d, v, cx):
    if randnum.randint(1,10) != 1:
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
        cx.write("oldSearch\t"+engine, 1)
        return
    try:
        searchEvents = UITelem["toolbars"]["countableEvents"]["__DEFAULT__"]["search"]
    except KeyError:
        pass
    else:
        for (key, value) in searchEvents.iteritems():
            if isinstance(value, int):
                cx.write("totalsearches-"+key+"\t"+engine, value)
                cx.write("totalsessions-"+key+"\t"+engine, 1)
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
                        cx.write("selectsearch-0\tALL", totaluse[sel])
                    elif (totaluse[sel] > 0):
                        cx.write("selectsessions-1+\t"+engine, 1)
                        cx.write("selectsearches-1+\t"+engine, totaluse[sel])
                        cx.write("selectsearch-"+str(sel)+"\tALL", totaluse[sel])
    
    
    

def reduce(k, v, cx):
    c = sum(v)
    cx.write(k.encode('utf8','xmlcharrefreplace'), c)

combine = reduce
