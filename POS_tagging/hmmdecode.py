import sys
from _collections import defaultdict
import math
import time

start_time = time.time()
emissionProbability = {}
transitionProbability = {}
tagsofword = {}
tagCount = {}
previousProbability = 1.0

previousTags = {}
currentTagProbability = {}
currentPreviousTags = {}
a_list = []
backtrackingDictionary = {}
countTags = 0
backDictionaryIndex = -1
a=[]
stringToWrite = ""
wordlist = []

def viterbi():
    global previousTags,countTags,backDictionaryIndex,words,f
    f = open('hmmoutput.txt', 'w+')
    with open(sys.argv[1], 'r') as my_file:
        for line in my_file:
            previousTags.clear()
            #print("--------------------------------------------------------------------")
            maximum = 0.0
            start = "<s>"
            end = "<e>"
            finalLine = start + " " + line;
            finalLine = finalLine.replace("\n"," <e>")# bringing all lines to format <s> line <e> to know starting and ending of line
            words = finalLine.strip().split(" ")
            #print(finalLine)
            for word in words:
                backDictionaryIndex += 1
                current = word.lower()
                #print("current word - " + current) # change each word to lowercase
                if word == '<s>':
                    previousTags[word] = 1.0 #previous Tag asociatted
                #elif word == '<e>':
                    #continue
                else:
                    if current not in tagsofword:
                        tagsofword[current] = set() 
                        #print(current  + " new word")
                        for tag in tagCount:
                            #print(tag)
                            if tag == '<s>':
                                continue
                            if tag == '<e>':
                                continue
                            tagsofword[current].add(tag)
                            #print(tagsofword[word])
                            emissionProbability[current] = emissionProbability.get(current,{})
                            temp = float(1)
                            emissionProbability[current][tag] = float(temp)
                    #print(tagsofword[current])
                    currentTags = emissionProbability[current].keys()# get all tags associated with word
                    #print("current tags")
                    #print(currentTags)
                    for tag in currentTags:
                        countTags += 1
                        #print("current tag - " + tag)
                        for previousTag in previousTags:
                            #print("previousTags - ")
                            #print(previousTags)
                            #print("current previousTag - " + previousTag)
                            if tag not in transitionProbability[previousTag].keys():
                                transitionProbability[previousTag] = transitionProbability.get(previousTag,{})
                                temp1 = float(1/(float(tagCount[previousTag]) + float(len(tagCount)-2)))
                                #print(temp1)
                                transitionProbability[previousTag][tag] = temp1
                                #print(transitionProbability)
                                #print("not found")
                            #else:
                            probabilityValue = math.log(transitionProbability[previousTag][tag]) + math.log(emissionProbability[current][tag]) + previousTags[previousTag]
                            currentTagProbability[previousTag] = probabilityValue# collect all probability values of tags to get maximum
                            #print("probabilityValue - " + str(probabilityValue))
                            #print("currentTagProbability")
                            #print(currentTagProbability)
                            #print(current + str(probabilityValue) + " " + tag)
                            #print(currentTagProbability)
                        #print(currentTagProbability)
                        maximum = max(currentTagProbability.values())
                        maxKeyValue = filter(lambda x:x[1] == maximum,currentTagProbability.items())
                        #maxKeyValue = max(currentTagProbability.iteritems(), k = lambda x: x[1])[0]
                        currentTagProbability.clear()
                        #print("maxKeyValue")
                        #print(maxKeyValue)
                        #print(tag)
                        backtrackingDictionary[backDictionaryIndex] = backtrackingDictionary.get(backDictionaryIndex,{})
                        backtrackingDictionary[backDictionaryIndex][tag] = maxKeyValue[0][0]
                        #print("backtracking dictionary")
                        #print(backtrackingDictionary)
                        currentPreviousTags[tag] = maxKeyValue[0][1] # tags that will be previous for next word
                        #print("currentPreviousTags - ")
                        #print(currentPreviousTags)
                        if countTags is len(currentTags):
                            #print("previousTags")
                            #print(previousTags)
                            del previousTags
                            previousTags = currentPreviousTags.copy()
                            countTags = 0
                            #print("Now previousTags")
                            #print(previousTags)

                    #print(emissionProbability[current].keys())
                    currentTagProbability.clear()
                    currentPreviousTags.clear()


            #print(finalLine)
            #print(words)
            writeToFile()


def writeToFile():
    global stringToWrite,backDictionaryIndex
    #print("BACK DIC IN write")
    #print(backtrackingDictionary)
    for i in range(len(backtrackingDictionary),0,-1):
        if i is len(backtrackingDictionary):
            a.append(backtrackingDictionary[i]["<e>"])
            toCheck = backtrackingDictionary[i]["<e>"]
        else:
            a.append(backtrackingDictionary[i][toCheck])
            toCheck = backtrackingDictionary[i][toCheck]
    b = a[::-1]
    #print(words)
    #print(b)
    for word in words:
        if word is '<s>':
            continue
        elif word is '<e>':
            continue
        else:
            wordlist.append(word)
    for i in range(1,len(b)):
        if i is len(b)-1:
            stringToWrite = stringToWrite + wordlist[i] + "/" + b[i] + "\n"
            #print("last word")
        else:
            stringToWrite = stringToWrite + wordlist[i] + "/" + b[i] + " "
    #print("STRING - " + stringToWrite)

    f.write(stringToWrite)
    backtrackingDictionary.clear()
    backDictionaryIndex = -1
    a[:] = []
    b[:] = []
    wordlist[:] = []
    #print(stringToWrite)
    stringToWrite = ''






def getValue(line,n):
    tokens = line.strip().split(" ")
    if n is 1:
        emissionProbability[tokens[0]] = emissionProbability.get(tokens[0],{})
        emissionProbability[tokens[0]][tokens[1]] = float(tokens[2])
    if n is 2:
        transitionProbability[tokens[0]] = transitionProbability.get(tokens[0],{})
        transitionProbability[tokens[0]][tokens[1]] = float(tokens[2])
    if n is 3:
        tags = tokens[1].split(",")
        tagsofword[tokens[0]] = set()
        for i in range(0,len(tags)):
            tagsofword[tokens[0]].add(tags[i])
    if n is 4:
        tagCount[tokens[0]] = tokens[1]

        #emission[splitspace[0]]=emission.get(splitspace[0],{})
        #emission[splitspace[0]][splitspace[1]]=emission[splitspace[0]].get(splitspace[1],float(splitspace[2]))

def main():
    n=0
    with open('hmmmodel.txt') as my_file:
        for line in my_file:
            if line.startswith("------Emission Probability------"):
                n=1
            elif line.startswith("------Transition Probability------"):
                n=2
            elif line.startswith("------Word Tags------"):
                n=3
            elif line.startswith("------Tag Count------"):
                n=4
            else:
                getValue(line,n)
    viterbi()

if __name__ == '__main__':
    main()
    """print("emissionProbability")
    print(emissionProbability)
    print("\ntransitionProbability")
    print(transitionProbability)
    print("\ntagsofword")
    print(tagsofword)
    print("\ntagCount")
    print(tagCount)
    print(backtrackingDictionary)
    print("\n")
    print(transitionProbability['<s>'].keys())"""
    print("\n\n--- %s seconds ---" % (time.time() - start_time))
