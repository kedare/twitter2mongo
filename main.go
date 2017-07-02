package main

import (
	"crypto/tls"
	"github.com/ChimeraCoder/anaconda"
	"github.com/Sirupsen/logrus"
	"github.com/mvdan/xurls"
	"gopkg.in/mgo.v2"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

var (
	consumerKey       = os.Getenv("TWITTER2MONGO_CONSUMER_KEY")
	consumerSecret    = os.Getenv("TWITTER2MONGO_CONSUMER_SECRET")
	accessToken       = os.Getenv("TWITTER2MONGO_ACCESS_TOKEN")
	accessTokenSecret = os.Getenv("TWITTER2MONGO_ACCESS_TOKEN_SECRET")
	mongoURL          = os.Getenv("TWITTER2MONGO_MONGODB_URL")
	mongoDB           = os.Getenv("TWITTER2MONGO_MONGODB_DB")
	trackList         = strings.Split(os.Getenv("TWITTER2MONGO_TRACK"), ",")
)

func main() {
	// Logging
	log := &Logger{
		&logrus.Logger{
			Out:   os.Stdout,
			Hooks: make(logrus.LevelHooks),
			Level: logrus.DebugLevel,
			Formatter: &logrus.TextFormatter{
				FullTimestamp: true,
			},
		},
	}

	// MongoDB
	mongoDialInfo, err := mgo.ParseURL(mongoURL)
	if err != nil {
		log.Panicf("Failed to parse MongoDB url: %s", err)
	}
	mongoDialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		return tls.Dial("tcp", addr.String(), &tls.Config{InsecureSkipVerify: true})
	}

	mongoDialInfo.Timeout = 2 * time.Second

	log.Println("Connecting to MongoDB")
	mongoSession, err := mgo.DialWithInfo(mongoDialInfo)
	if err != nil {
		log.Panicf("Failed to connect to MongoDB: %s", err)
	}
	log.Info("Connected to MongoDB")
	collection := mongoSession.DB(mongoDB).C("tweets")

	// Twitter
	log.Info("Connecting to Twitter")
	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecret)
	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)

	api.SetLogger(log)

	log.Println("Connected to Twitter")

	// Streaming
	stream := api.PublicStreamFilter(url.Values{
		"track": trackList,
	})

	defer stream.Stop()

	log.Println("Streaming tweets")

	for tweetRaw := range stream.C {
		tweet, ok := tweetRaw.(anaconda.Tweet)
		if !ok {
			log.Errorf("Received unexpected value of type %T", tweetRaw)
			continue
		}
		go processTweet(log, collection, tweet)
	}
}

func processTweetedURL(log *Logger, tweet anaconda.Tweet, url string) error {
	return nil
}

func processTweet(log *Logger, collection *mgo.Collection, tweet anaconda.Tweet) error {
	log.Infof("Processing tweet #%s from %s", tweet.IdStr, tweet.User.Name)
	// Extract urls from the tweet
	urls := xurls.Relaxed.FindAllString(tweet.Text, -1)

	err := collection.Insert(tweet)
	if err != nil {
		log.Fatalf("Failed to save tweet #%s: %s", tweet.IdStr, err)
	}

	for _, url := range urls {
		go processTweetedURL(log, tweet, url)
	}

	return nil
}

type Logger struct {
	*logrus.Logger
}

func (log *Logger) Critical(args ...interface{})                 { log.Error(args...) }
func (log *Logger) Criticalf(format string, args ...interface{}) { log.Errorf(format, args...) }
func (log *Logger) Notice(args ...interface{})                   { log.Info(args...) }
func (log *Logger) Noticef(format string, args ...interface{})   { log.Infof(format, args...) }
