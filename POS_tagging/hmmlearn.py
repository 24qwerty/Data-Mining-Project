# -*- coding: UTF-8 -*-
#!/usr/bin/env python2

import sys
from _collections import defaultdict
import time


start_time = time.time()
tagcountemit = defaultdict(int)
wordgiventagemit = defaultdict(int)
tagGivenTagTransmit = defaultdict(int)
tagcounttransmit = defaultdict(int)
emitionProbability = defaultdict(int)
transitionProbability = defaultdict(int)
wordTags = {}
tagList = []
tagsofword = {}
uniqueWords = set()

def main():
    with open(sys.argv[1], 'r') as my_file:
        for line in my_file:
            start = "<s>/<s>"
            end = "<e>/<e>"
            finalLine = start + " " + line;
            finalLine = finalLine.replace("\n"," <e>/<e>")
            tokens = finalLine.strip().split(" ")#split individual words
            #print(finalLine)
            for token in tokens:
                a = token.rsplit("/",1)#split at last occurance of /
                #print(a)
                if a[1] in tagcountemit:#count number of tags
                    tagcountemit[a[1]] += 1
                else:
                    tagcountemit[a[1]] = 1
                smallWord = a[0].lower()#convert to lower case
                combineWord = smallWord + "/" + a[1]
                uniqueWords.add(smallWord)#Generate list of all unique words

                if combineWord in wordgiventagemit:
                    wordgiventagemit[combineWord] += 1
                else:
                    wordgiventagemit[combineWord] = 1
                if a[1]=="<s>":
                    previousTag = "<s>"
                    continue
                else:
                    newTag = previousTag + "/" + a[1]
                    previousTag = a[1]
                    if newTag in tagGivenTagTransmit:
                        tagGivenTagTransmit[newTag] += 1
                    else:
                        tagGivenTagTransmit[newTag] = 1

        #print("\n")
        for word in tagGivenTagTransmit:
            b = word.rsplit("/",1)
            if b[1] == "<e>":
                #print(b)
                temp = tagcountemit[b[0]] - tagGivenTagTransmit[word]
                if temp == 0:
                    temp = 1
                #print(temp)
                tagcounttransmit[b[0]] = temp
            else:
                continue

        for tag in tagcountemit:
            if tag in tagcounttransmit:
                continue
            else:
                tagcounttransmit[tag] = tagcountemit[tag]


    """print(tagcountemit)
    print("\n")
    print(wordgiventagemit)
    print("\n")
    print(tagGivenTagTransmit)
    print("\n")
    print(tagcounttransmit)
    print(len(tagcounttransmit))"""


    #Emition Probability Calculation
    uniqueWordsLength = (len(uniqueWords) - 2)
    for word,v in wordgiventagemit.iteritems():
        b = word.rsplit("/",1)
        emitionProbability[word] = ((float(wordgiventagemit[word]) + 0.0) / (float(tagcountemit[b[1]])))
        #temp = float(wordgiventagemit[word]) / float(tagcountemit[b[1]])
        #p = (1/(tagcountemit[b[1]] + uniqueWordsLength))
        #if temp > 0:
            #emitionProbability[word] = temp - p
        #else:
            #emitionProbability[word] =


    uniqueTagsLength = (len(tagcounttransmit) - 2)
    for word in tagGivenTagTransmit:
        b = word.rsplit("/",1)
        transitionProbability[word] = ((float(tagGivenTagTransmit[word])+ 1.0) / (float(tagcountemit[b[0]]) + uniqueTagsLength))
        #transitionProbability[word] = float(tagGivenTagTransmit[word]) / float(tagcountemit[b[0]])
        a = str(transitionProbability[word])
        #print(a + " " +word)

    """print("\n Emition prob")
    print(emitionProbability)
    print("\n Transition Prob")
    print(transitionProbability)"""
    #getting tags for all words
    write()

def write():
     with open("hmmmodel.txt", 'w') as f:
         #emission Probability
        f.write("------Emission Probability------\n")
        for word in emitionProbability:
            b = word.rsplit("/",1)
            f.write(str(b[0]) + " " + str(b[1]) + " " + str(emitionProbability[word]) + "\n")

        #transition Probability
        f.write("------Transition Probability------\n")
        for word in transitionProbability:
            b = word.rsplit("/",1)
            f.write(str(b[0]) + " " + str(b[1]) + " " + str(transitionProbability[word]) + "\n")
            #print(word)

        #word tags
        f.write("------Word Tags------\n")
        for word in emitionProbability:
            a = word.rsplit("/",1)
            if a[0] not in tagsofword:
                tagsofword[a[0]] = set()
            tagsofword[a[0]].add(a[1])
        #print(tagsofword)
        for key, value in tagsofword.items():
            f.write(key + " " + ','.join([str(i) for i in value]) + "\n")

        #tag Counts
        f.write("------Tag Count------\n")
        for word in tagcountemit:
            f.write(word + " " + str(tagcountemit[word]) + "\n")

if __name__ == "__main__":
    main()
    #print("ads")
    #print(len(uniqueWords))
    print("\n\n--- %s seconds ---" % (time.time() - start_time))
