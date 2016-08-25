from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import re


def main():
    
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec    
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")    
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    import itertools
    counts = list(itertools.chain(*counts))
    
    positive = [x[1] for x in counts if x[0]=='positive']
    negative = [x[1] for x in counts if x[0]=='negative']

    p_plot, = plt.plot(range(0,len(positive)), positive, 'go-')
    n_plot, = plt.plot(range(0,len(negative)), negative, 'ro-')
    plt.legend((p_plot, n_plot), ('Positive', 'Negative'), loc='upper left')
    plt.ylabel('Word Count')
    plt.xlabel('Time step')
    plt.xlim([-1,len(positive)+1])
    plt.show()
    
def load_wordlist(filename):
    """ 
        This function should return a list or set of words from the given filename.
    """
    word_file = open(filename)
    words = word_file.read().split("\n")
    return words

def common(x,y):
    cnt = list(set(x) & set(y))
    return cnt

def sanitize(x):
    lst = x.split(" ")
    lst = [re.sub("[^a-zA-Z ]","", x).strip().lower() for x in lst]
    lst = filter(None, lst)

    return lst

def updateFunc(curr, total):
    if(total is None):
        total = 0
    return sum(curr,total)
def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: str(x[1].encode("ascii","ignore")))
    
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    #ini = sc.parallelize([('positive', 0), (u'negative', 0)])
    words_in_tweets = tweets.map(lambda tweet: (tweet, sanitize(tweet))).flatMapValues(lambda x: x).map(lambda x: x[1])
    #words_in_tweets.pprint()
    #positive_count = words_in_tweets.map(lambda x: (x[0],count(x[1],pwords)))
    positives = words_in_tweets.filter(lambda x: x in pwords)
    negatives = words_in_tweets.filter(lambda x: x in nwords)
    
    positives = positives.map(lambda x:("positive",1))
    negatives = negatives.map(lambda x:("negative",1))

    total = positives.union(negatives)
    curr_count = total.reduceByKey(lambda x,y: x+y)
    cum_count = total.updateStateByKey(updateFunc)
    
    cum_count.pprint()
    counts = []
    # # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    curr_count.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    #counts = []
    ssc.start()                                     # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts

if __name__=="__main__":
    main()
