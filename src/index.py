import os, json
from datetime import datetime, timedelta
import pandas as pd
from decouple import config

def convertMillis(input, includeMs):
    try:
        ms = int(input)
        seconds, millis = divmod(ms, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 24)
        if (includeMs):
            return "{}:{:02}:{:02}.{:03}".format(hours, minutes, seconds, millis)
        else:
            return "{}:{:02}:{:02}".format(hours, minutes, seconds)
    except:
        print('Bad input for milliseconds')

try:
    threshold = int(config('GAP_THRESHOLD'))
except:
    print('Invalid threshold given, defaulting to 5000')
    threshold = 5000
    
path_to_json = config('INPUT_DIR')
ext_from_config = config('EXT')
ext = tuple(map(str, ext_from_config.split(',')))
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith(ext)]
files_with_errors = []
jsons_data = pd.DataFrame(columns=['filename', 'gapMs', 'gap', 'gapStartTimeMs', 'gapStartTime', 'gapStopTimeMs', 'gapStopTime'])

for index, js in enumerate(json_files):
    with open(os.path.join(path_to_json, js), encoding='utf-8-sig') as json_file:
    #with open(os.path.join(js)) as json_file:
        print('Opening for processing:  [' + str(index) + '] ' + js)
        try: 
            json_text = json.load(json_file)
        except:
            print('Cannot load file as json. Skipping')
            files_with_errors.append(js)
            continue
        #json_text = json.load(json_file.read().decode('utf-8-sig'))
        
        for indexMs, timeSeries in enumerate(json_text):
            #print(indexMs)
            #print(timeSeries)
            if (indexMs == len(json_text)-1):
                break
            gapStartTimeMs = timeSeries['stopTimeMs']
            gapStopTimeMs = json_text[indexMs + 1]['startTimeMs']
            gapMs = gapStopTimeMs - gapStartTimeMs
            if (gapMs > threshold):
                #print("Gap detected: [", str(gap), "]:")
                #print(str(gapStartTimeMs), " - ", str(gapStopTimeMs))
                gap = convertMillis(gapMs, False)
                gapStartTime = convertMillis(gapStartTimeMs, False)
                gapStopTime = convertMillis(gapStopTimeMs, False)
                filename = os.path.basename(js)
                jsons_data.loc[indexMs] = [filename, gapMs, gap, gapStartTimeMs, gapStartTime, gapStopTimeMs, gapStopTime]

if jsons_data.empty:
    print('No gaps found in collection')
else:
    print(jsons_data)         
    jsons_data.to_csv('output/Veritone_GapAnalysis.csv', index=False)
if len(files_with_errors) > 0:
    print('The following files were skipped due to errors:  ')
    print(files_with_errors)
print('Done!')