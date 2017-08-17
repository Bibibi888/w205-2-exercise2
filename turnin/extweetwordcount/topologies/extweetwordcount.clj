(ns wordcount
  (:use     [streamparse.specs])
  (:gen-class))

(defn wordcount [options]
   [
    ;; spout configuration
    {"tweet-spout" (python-spout-spec
          options
          "spouts.tweets.Tweets"
          ["tweets"]
          :p 3
          )
    }
    ;; bolt configuration
    {"parse-tweet-bolt" (python-bolt-spec
          options
          {"tweet-spout" :shuffle}
          "bolts.parse.ParseTweet"
          ["parsedword"]
          :p 3
          )
    ;; bolt configuration - out
    "count-bolt" (python-bolt-spec
          options
          {"parse-tweet-bolt" :shuffle} 
          "bolts.tweetcounter.TweetCounter"
          ["parsedword" "count"]
          :p 2
          )
    }
  ]
)
