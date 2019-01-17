import pandas as pd
import re
import csv
pd.set_option('display.max_colwidth', 500)
pd.set_option('display.max_rows', 4000)
pd.set_option('display.max_columns', 100)
units = ['meq l', 'umol l', 'g m', 'mol l','mol l', 'm', 'v', 'mg kg', 'mg g', 'mol m s',
         'ueql', 'degc', 'degress', 'degreecelsius', 'degreescelsius', 'celsius', 'deg', 'gl', 'permil', 'mgkg', 'cm',
         'kpa', 'mgl', 'ugml', 'umol/l', 'mol/l', 'meql1', 'meql', 'moll1', 'nm', 'cm2', 'ms1', 'ms', 'm3day', 'm3',
         'gm3', 'lbs', 'mv', 'mg', 'mgg', 'gcm3', 'ugl', 'g', 'ngl', 'ppm', 'meterspersecond', 'cm3persecond', 'wm2',
         'molm2s1', 'umolm2', 'm3s', 'cfs', 'ft', 'ls', 'uscm', 'mm', 'km', 'km3', 'kmhr', 'mmhr', 'gm2']

aggs = ['avg', 'average', 'minimum', 'minimumtimeof', 'std', 'stddev', 'stdev', 'standarddeviation',
         'standarddeviationof', 'standarddeviationofmean', 'max', 'maximum', 'maxtimeof', 'median', 'medianof',
         'medianofmaximum', 'medianmaximum', 'medianofmean', 'medianofminimum', 'mediantimeofmaximum',
         'mediantimeofminimum', 'meanofmaximum', 'meanofmean', 'meanofminimum', 'meanofstandarddeviation',
         'meanstandarddeviationof', 'vectormean', 'scalarmean', 'timeofminimum', 'timeofmaximum', 'sum', 'sumof',
         'total', 'count']
df = pd.read_csv("czo-dataset-metadata-2018-12-17-utf8-short.csv")
key = df['VARIABLES']
czo_id = df['czo_id']
keys = []
for i in range(0, len(key)):
    row = key[i].split('|')

    for j in range(0, len(row)):
        k = row[j].lower()

        startingSpacesRemoved = re.sub('\s*$', '', k)
        endingSpacesRemoved = re.sub('^\s*', '', startingSpacesRemoved)
        cleanString = re.sub('[^a-zA-Z0-9\s]+', ' ', endingSpacesRemoved)
        cleanString = cleanString.replace("\t", ' ')
        # print(cleanString + ' ' + str(czo_id))
        keys.append((cleanString, czo_id[i]))

d1 = {}
d2 = []
seen = set()
count = 0
for i in range(len(keys)):
    word = keys[i]
    if word[0] not in seen:
        seen.add(word[0])

        for j in range(len(keys)):
            a_duplicate = keys[j][0]
            if (word[0] == a_duplicate):
                count += 1
        d1[word] = count
        count = 0
    # print(word)
    if not word[0].strip() == '':
        d2.append(word)


d_sorted1 = sorted(d1.items(), key=lambda x: (-x[1], x[0]))

seenList = sorted(seen)

with open('variablesOnly2.txt','w') as f:
    f.write('\n'.join('%s' % x for x in seenList))

#Run this to create a txt file and store the list
d2=sorted(d2)
with open('wczo_id12-17-18-3.csv', 'w') as fp:
    fp.write('variable,czo_id,\n')
    fp.write('\n'.join('%s,%s' % x for x in d2))


czo_vars = pd.read_csv('wczo_id12-17-18-3.csv',sep =',')
# originaldata = pd.read_csv("CZO_data_parsing/czo-datasets-metadata-2018-03-01c.csv", index_col='uuid')
# czo-dataset-metadata-2018-12-17.csv
originaldata = pd.read_csv("czo-dataset-metadata-2018-12-17-utf8-short.csv", index_col='uuid')
print(list(originaldata))
print(list(czo_vars))
czo_vars['Discipline'] = ''#originaldata['DISCIPLINES']
czo_vars['CZOs'] = ''
# czo_vars['Discipline'] = np.where(czo_vars['czo_id'] == originaldata['czo_id'], originaldata['DISCIPLINES'])
for index, ogrow in originaldata.iterrows():
    # print(ogrow['czo_id'])
    for index2, czorow in czo_vars.iterrows():
        # print(czorow[' czo_id'])
        if ogrow['czo_id'] == czorow['czo_id']:
            czo_vars.set_value(index2, 'Discipline', ogrow['DISCIPLINES'])
            czo_vars.set_value(index2, 'CZOs', ogrow['CZOS'])
print(czo_vars.head())
czo_vars.to_csv('wczo_id_with_terms_discp12-17-18-4.csv',sep =',')

new_czo_vars = pd.read_csv('wczo_id_with_terms_discp12-17-18-4.csv',sep =',')
old_czo_vars = pd.read_csv('classifiedCZOVariables2.csv',sep =',')
# variable	czo_id	Discipline	CZOs
fields =['variable','ODM2Name','ODM2Term','CZO Variable Action','New ODM2 Term',
         'comments','Discipline','CZOs','units','Aggregation','czo_id']
writeheader = True
with open('wczo_id_with_terms_discp12-17-18-4.csv',mode='r') as new_czo_vars:
    with open('new_czo_vars_missing.csv', 'w', newline='') as out:
        newreader = csv.reader(new_czo_vars)
        for newrow in newreader:
            #print(newrow[1])
            newvar = newrow[1]
            newczo_id = newrow[2]
            newdiscipline = newrow[4]
            czo = newrow[5]
            found = False
            foundChunk = False
            with open('classifiedCZOVariables2.csv', mode='r') as old_czo_vars:
                oldreader = csv.reader(old_czo_vars)
                biggestMatch = 0
                oldBiggestMatch = 0
                chunkedoutvar = {}
                flag = False
                for oldrow in oldreader:
                    # print(oldrow)
                    outvar = {}
                    # print(newvar)
                    startingSpacesRemoved = re.sub('\s*$', '', newvar)
                    endingSpacesRemoved = re.sub('^\s*', '', startingSpacesRemoved)
                    newvalstripped = re.sub(' ', '', endingSpacesRemoved).lower()
                    pat = re.compile(" ")
                    var_chunks = pat.split(endingSpacesRemoved.lower())

                    ogstartingSpacesRemoved = re.sub('\s*$', '', oldrow[1])
                    ogendingSpacesRemoved = re.sub('^\s*', '', ogstartingSpacesRemoved)
                    oldvalstripped = ogendingSpacesRemoved.lower() # re.sub(' ', '', ogendingSpacesRemoved)
                    old_var_chunks = pat.split(oldvalstripped)

                    #print(oldvalstripped)
                    #print(newvalstripped)
                    outvar['Discipline'] = newdiscipline
                    outvar['variable'] = newvar
                    outvar['czo_id'] = newczo_id
                    outvar['CZOs'] = czo
                    if oldvalstripped == newvalstripped:
                        #print('new: ' + newvalstripped)
                        #print('old: ' + oldvalstripped)
                        found = True
                        outvar['ODM2Name'] = oldrow[5]
                        outvar['ODM2Term'] =  oldrow[6]
                        outvar['CZO Variable Action'] =  oldrow[7]
                        outvar['New ODM2 Term'] =  oldrow[8]
                        outvar['comments'] =  oldrow[9]
                        outvar['units'] =  oldrow[10]
                        outvar['Aggregation'] =  oldrow[11]
                        break
                    if not found:
                        # var_chunks = ''
                        var_chunked = ''
                        old_var_chunked = ''
                        if newvar == ' co2 20 hi ':
                            print('HERE')
                            flag = True
                            print(var_chunks)
                            print(oldvalstripped)

                        for var_chunk in var_chunks:
                            oldchunkcount = 0
                            #foundChunk = False
                            localFound = False
                            for old_var_chunk in old_var_chunks:
                                oldchunkcount +=1
                                var_chunked = var_chunked + ' ' + var_chunk
                                old_var_chunked = old_var_chunked + ' ' +old_var_chunk
                                unitOrAgg = False
                                unitOrAggChunk = False
                                if old_var_chunked in aggs or old_var_chunked in units:
                                    unitOrAgg = True
                                if old_var_chunk in aggs or old_var_chunk in units:
                                    unitOrAggChunk = True
                                if oldvalstripped == var_chunk or oldvalstripped == var_chunked:
                                    localFound=True
                                if var_chunked == old_var_chunked and not unitOrAgg:
                                    localFound = True
                                if var_chunked == old_var_chunk and not unitOrAggChunk:
                                    localFound = True
                                if localFound:
                                    # print('chunk find')
                                    # print(oldvalstripped)
                                    outvar['ODM2Name'] = oldrow[5]
                                    outvar['ODM2Term'] = oldrow[6]
                                    outvar['CZO Variable Action'] = oldrow[7]
                                    outvar['New ODM2 Term'] = oldrow[8]
                                    outvar['comments'] = oldrow[9]
                                    outvar['units'] = oldrow[10]
                                    outvar['Aggregation'] = oldrow[11]
                                    if oldchunkcount > biggestMatch:
                                        chunkedoutvar = outvar
                                        print(newvar)
                                        print(old_var_chunked)
                                        foundChunk = True
                                        if flag:
                                            print('FLAG')
                                            print(chunkedoutvar)
                                            print(outvar)
                                            flag=True
                                        biggestMatch = oldchunkcount

                w = csv.DictWriter(out, fieldnames=fields)
                if writeheader:
                    w.writeheader()
                    writeheader = False
                if not found and foundChunk:
                    #print('outvar')
                    #print(outvar)
                    #print('chunked')
                    #print(chunkedoutvar)
                    outvar = chunkedoutvar
                w.writerow(outvar)