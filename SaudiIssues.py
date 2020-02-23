#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
#import re
import tweepy
import time
from itertools import islice

from itertools import islice


def authenticate(tokens):
    print(tokens[0])
    consumer_key = tokens[0]
    consumer_secret = tokens[1]
    access_token = tokens[2]
    access_token_secret = tokens[3]
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api


tokenList = iter([
                 # Tokens
                 ])


def getNextToken():
    global tokenList
    try:
        return next(tokenList)
    except:
        # if you reach the end of the iterator, re-initialize and iterate again
        tokenList = iter([
            # Tokens
        ])
        return getNextToken()


def reAuthenticate():
    print('Trying new credentials...')
    time.sleep(10)
    global api
    nextToken = getNextToken()
    api = authenticate(nextToken)


counter = 1
#4.1, 4.2


def getStats(status, text, senti):
    global counter
    print("Making a request: " + str(counter))
#    print(text[0:3])
#    print('Reached data limit, trying new credentials...')
#    reAuthenticate()
    try:
        if 'RT' in text:
            print('Retweet, skipping..')
            return None
        else:
            profile = api.get_status(status)._json
            counter += 1
            _id = profile['user']['id_str']
            status_id = profile['id_str']
            name = profile['user']['screen_name']
    #        stats.append(profile)
            with open("SaudiIssuesStatusesNew.json", "a") as json_file:
                json_file.write(json.dumps(
                    profile, ensure_ascii=False).encode('utf8')+'\n')
            addUniqueUsers(_id, name)
            getMetaData(profile, _id)
            likes = profile["favorite_count"]
            retweets = profile['retweet_count']
            addStatus(_id, status_id, retweets, likes, senti)
            return likes, retweets, True
    except Exception as e:
        #        suspended, deleted, unavailable, etc
        print("Rejected: " + status)
        print(repr(e))
        failedStats = {'screen_name': findTweet(
            status, False), 'staus_id': status, 'error_msg': repr(e)}
        with open("SaudiIssuesFailedStatuses.json", "a") as json_file:
            json_file.write(json.dumps(
                failedStats, ensure_ascii=False).encode('utf8') + '\n')
        if "rate limit" in repr(e).lower() or "not authenticate" in repr(e).lower() or "429" in repr(e).lower():
            # if the rate limit is reached, re-atuhenticate with a new token and try again
            reAuthenticate()
            return getStats(status, text, senti)
        return None


def findTweet(status_id, return_senti=False, return_obj=False):
    for item in all_tweets:
        #print(item['Status Id'])
        if item['status_id'] == status_id:
            if return_senti:
                return item['sentiment_analysis']
            elif return_obj:
                return item
            else:
                return item['screen_name']
#    print("not found")
    return False


#    4.4.1
#    creation date, number of followers, number of follows,
#    number of tweets, number of likes, number of replies, goe location
def getMetaData(profile, _id):
    uniqueUsersList[_id]['creation_date'] = profile['user']['created_at']
    uniqueUsersList[_id]['follower_count'] = profile['user']['followers_count']
    uniqueUsersList[_id]['tweet_count'] = profile['user']['statuses_count']
    uniqueUsersList[_id]['likes_count'] = profile['user']['favourites_count']
    uniqueUsersList[_id]['goe_location'] = profile['user']['location']
    if profile['user']['created_at'][-4:] == '2019':
        freshUsersList[_id] = uniqueUsersList[_id]


# 4.3
def addUniqueUsers(_id, name):
    if _id not in uniqueUsersList.keys():
        uniqueUsersList[_id] = {'screen_name': name, 'user_id': _id, 'statuses': {
        }, 'total_retweets': 0, 'total_likes': 0}


def addStatus(_id, status, retweets, likes, sentiment):
    if status not in uniqueUsersList[_id]['statuses'].keys():
        uniqueUsersList[_id]['statuses'][status] = {
            'retweets': retweets, 'likes': likes, 'sentiment': sentiment}
        uniqueUsersList[_id]['total_retweets'] += retweets
        uniqueUsersList[_id]['total_likes'] += likes

    else:
        uniqueUsersList[_id]['statuses'][status]['retweets'] = max(
            retweets, uniqueUsersList[_id]['statuses'][status]['retweets'])
        uniqueUsersList[_id]['statuses'][status]['likes'] = max(
            likes, uniqueUsersList[_id]['statuses'][status]['likes'])


# we count the volume for duplicates, for likes & retweets we use sets so we dont add them twice
def addToHashtagsDict(hashtags, hashtags_set, retweets, likes):
    for hashtag in hashtags:
        if hashtag in hashtagDict.keys():
            hashtagDict[hashtag]['volume'] += 1
        else:
            hashtagDict[hashtag] = {'volume': 1,
                                    'total_retweets': 0, 'total_likes': 0}

    for hashtag in hashtags_set:
        hashtagDict[hashtag]['total_retweets'] += retweets
        hashtagDict[hashtag]['total_likes'] += likes


def addToHashtagSet(tweet):
    _set = set()  # {tweet['Hashtags'][0], tweet['Hashtags'][1]}
#    hashtagSet['sum'] +=1
    for tag in tweet['hashtags']:
        _set.add(tag)
    if (_set in hashtags):
        hashtagSet[hashtags.index(_set)].append(tweet['status_id'])
    else:
        #        if len(_set) == 2:
        hashtags.append(_set)
        hashtagSet[hashtags.index(_set)] = [tweet['status_id']]


def getTimeLines(user_id):
    results = tweepy.Cursor(
        api.user_timeline, user_id=user_id, tweet_mode="extended").items()
    try:
        status_list = []
        for status in results:
            status_list.append(status._json)
        freshUsersList[user_id]['Timeline'] = status_list

    except Exception as e:
        print(repr(e))


# 88: rate limit, 32: authentication
def testLimit():
    try:
        while True:
            for tweet in all_tweets:
                print('request')
                profile = api.get_status(tweet['status_id'])._json
    except Exception as e:
        print(repr(e))
        reAuthenticate()
        testLimit()


uniqueUsersList = {}
# with open('hashtagDict_71678.json', 'w') as json_file:
#    json.dump(hashtagDict, json_file, ensure_ascii = False, indent=2)
freshUsersList = {}
failedStats = []
stats = []
hashtags = []
api = authenticate(getNextToken())
tweets = []
hashtagSet = {}
hashtagDict = {}
lines = []

# si all: 122020
all_tweets = []
file_name = "Project73.json"
file = json.loads(open(file_name, 'r').read())
#
#
for line in file:
    if len(line) == 1:
        all_tweets.append(line[0])
    else:
        print(len(line))

# to read the original file
# try:
#    for line in file:
#        #removes brackets at the star and end of each line
#        lines.append(json.loads(line[1:-2]))
# except:
#    # we actually need the bracket for the very last one, so we include it
#    lines.append(json.loads(line[1:-1]))
# 102574
#  71678  unique tweets: 26283 21.5%
for tweet in all_tweets:

    #    tweet = line
    #    all_tweets.append(tweet)
    temp = getStats(tweet['status_id'], tweet['text']
                    [0:3], tweet['sentiment_analysis'])

    if temp:
        tweet['likes_count'], tweet['retweet_count'], tweet['available'] = temp
        if 'hashtags' in tweet.keys():
            addToHashtagsDict(tweet['hashtags'], set(
                tweet['hashtags']), tweet['retweet_count'], tweet['likes_count'])
        tweets.append(tweet)
        with open("SaudiIssuesNoneRetweets.json", "a") as json_file:
            json_file.write(json.dumps(
                tweet, ensure_ascii=False)+'\n')
    else:
        tweet['available'] = False
        tweet['likes_count'] = 0
        tweet['retweet_count'] = 0

    with open("SaudiIssuesAllTweets.json", "a") as json_file:
        json_file.write(json.dumps(
            tweet, ensure_ascii=False) + '\n')

# for user in uniqueUsersList:
#    for key, value in uniqueUsersList[user]['statuses'].items():
#        value['sentiment'] = findTweet(key, True)


def sort_users(_list, n):
    return_list_retweets = dict(islice(
        sorted(_list.items(), key=lambda x: x[1]['total_retweets'], reverse=True), n))
    return_list_likes = dict(islice(
        sorted(_list.items(), key=lambda x: x[1]['total_likes'], reverse=True), n))
    return_list_total = dict(islice(sorted(
        _list.items(), key=lambda x: x[1]['totla_impressions'], reverse=True), n))
    return return_list_retweets, return_list_likes, return_list_total

#retweets_top_list, likes_top_list, total_top_list = sort_users(uniqueUsersList, 10)


with open("SaudiIssuesUniqueUsers.json", "w") as json_file:
    json.dump(uniqueUsersList, indent=2)
with open("SaudiIssuesHashtagsDict.json", "w") as json_file:
    json.dump(hashtagDict, json_file, indent=2)


# for key, value in tags.items():
#    write_tags[key] = value['volume']
#
# def sort_users(_list):
#    return_list = dict(sorted(_list.items(), key=lambda x:x[1], reverse=True))
#    return return_list
##test = sort_users(write_tags)

#%%
file = open('SaudiIssuesNoneRetweets.json', 'r').readlines()
tweets = []
for line in file:
    tweets.append(json.loads(line))

unique_users = json.loads(open('SaudiIssuesUniqueUsers.json', 'r').read())

hashtags_dict = json.loads(open('SaudiIssuesHashtagsDict.json', 'r').read())

group_1_tweets = json.loads(open('GroupOneTweets.json', 'r').read())
group_2_tweets = json.loads(open('GroupTwoTweets.json', 'r').read())

# uu = unique users
group_1_uu = {}
group_2_uu = {}


for tweet in group_1_tweets:
    _id = tweet['id_str']
    if _id in group_1_uu.keys():
        group_1_uu[_id]['statuses'][tweet['status_id']] = {'retweets': tweet['retweet_count'], 'likes': tweet['likes_count'],
                                                           'sentiment': tweet['sentiment_analysis'], 'link': 'twitter.com/'+tweet['screen_name']+'/status/'+tweet['status_id']}
        group_1_uu[_id]['total_retweets'] += tweet['retweet_count']
        group_1_uu[_id]['total_likes'] += tweet['likes_count']
        group_1_uu[_id]['total_impressions'] += tweet['likes_count'] + \
            tweet['retweet_count']
    else:
        group_1_uu[_id] = {'screen_name': tweet['screen_name'], 'statuses': {tweet['status_id']: {'retweets': tweet['retweet_count'], 'likes': tweet['likes_count'],
                                                                                                  'sentiment': tweet['sentiment_analysis'], 'link': 'twitter.com/'+tweet['screen_name']+'/status/'+tweet['status_id']}}, 'total_retweets': 0, 'total_likes': 0, 'total_impressions': 0}

for tweet in group_2_tweets:
    _id = tweet['id_str']
    if _id in group_2_uu.keys():
        group_2_uu[_id]['statuses'][tweet['status_id']] = {'retweets': tweet['retweet_count'], 'likes': tweet['likes_count'],
                                                           'sentiment': tweet['sentiment_analysis'], 'link': 'twitter.com/'+tweet['screen_name']+'/status/'+tweet['status_id']}
        group_2_uu[_id]['total_retweets'] += tweet['retweet_count']
        group_2_uu[_id]['total_likes'] += tweet['likes_count']
        group_2_uu[_id]['total_impressions'] += tweet['likes_count'] + \
            tweet['retweet_count']
    else:
        group_2_uu[_id] = {'statuses': {'retweets': tweet['retweet_count'], 'likes': tweet['likes_count'], 'sentiment': tweet['sentiment_analysis'], 'link': 'twitter.com/'+tweet['screen_name'] +
                                        '/status/'+tweet['status_id']}, 'total_retweets': tweet['retweet_count'], 'total_likes': tweet['likes_count'], 'total_impressions': tweet['retweet_count']+tweet['likes_count']}


def sort_list(_list, n):
    return_list = list(islice(
        sorted(_list, key=lambda x: x['retweet_count'], reverse=True), n))

    return return_list


t_1 = sort_list(group_1_tweets, 50)
t_2 = sort_list(g1tweets, 1200)

with open('GroupOneMostActiveUsers.json', 'w') as file:
    json.dump(z_1, file, ensure_ascii=False, indent=2)

with open('GroupTwoMostActiveUsers.json', 'w') as file:
    json.dump(z_2, file, ensure_ascii=False, indent=2)

with open('GroupOneTopTweets.json', 'w') as file:
    json.dump(t_1, file, ensure_ascii=False, indent=2)
with open('excel_tweets.json', 'w') as file:
    json.dump(t_2[1000:1050], file, ensure_ascii=False, indent=2)

r = 0
l = 0
hamad = []
for tweet in group_2_tweets:
    hamad.append({'sentiment': tweet['sentiment_analysis'], 'volume': tweet['retweet_count'] +
                  tweet['likes_count']+1, 'created_at': tweet['created_at']})


with open('HamanG2.json', 'w') as file:
    json.dump(hamad, file, ensure_ascii=False, indent=2)
with open('hashtags.json', 'w') as file:
    json.dump(t_2, file, ensure_ascii=False, indent=2)
print(r, l)


# for key, value in tags.items():
#    write_tags[key] = value['volume']
#
# def sort_users(_list):
#    return_list = dict(sorted(_list.items(), key=lambda x:x[1], reverse=True))
#    return return_list
##test = sort_users(write_tags)
'''
1
السعوديه_للسعوديين
ابناء_المواطنات_السعوديات
أبناء_المواطنات_أبنائنا
السعودية_للسعوديين
ابناء_المواطنات_ابنائنا
تجنيس_أبناء_السعوديات
ابناء_المواطنات
ابناء_المواطنات_ابنائنا
ابناء_المواطنات
لا_للتجنيس
مواليد_السعودية


2
'''
none = open('SaudiIssues/SaudiIssuesNoneRetweets.json', 'r').readlines()
_list = []
for line in none:
    _list.append(json.loads(line))
g1tweets = []
group1 = ['أبناء_المواطنات_أبنائنا', 'ابناء_المواطنات_السعوديات', 'السعوديه_للسعوديين', 'السعودية_للسعوديين',
          'ابناء_المواطنات_ابنائنا', 'تجنيس_أبناء_السعوديات', 'ابناء_المواطنات', 'لا_للتجنيس', 'مواليد_السعودية']
#
group2 = ['العلاوه_السنويه91', 'العلاوه_السنويه106', 'العلاوه_السنويه92', 'وزارة_التعليم', 'العلاوه_السنويه107', 'العلاوه_السنويه105', 'العلاوه_السنويه109',
          'العلاوه_السنويه108', 'العلاوه_السنويه104', 'العلاوه_السنويه99', 'العلاوه_السنويه90', 'العلاوه_السنويه96', 'العلاوه_السنويه97', 'العلاوه_السنويه100']
#
for tweet in _list:
    for hashtag in group2:
        if 'hashtags' in tweet.keys():
            if hashtag in tweet['hashtags']:
                g1tweets.append(tweet)
                break

with open('GroupTwoTweets2.json', 'w')as json_file:
    json.dump(g1tweets, json_file, ensure_ascii=False, indent=2)
