package main

import (
	"context"
	"fmt"
	"github.com/EgorBessonov/db-lab/lab_2/config"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"time"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Fatal("producer: %s", err)
	}
	redisClient, err := newRedisClient(cfg)
	if err != nil {
		logrus.Fatal("producer: %s", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logrus.Errorf("producer: error while closing redis client - %s", err)
		}
	}()
	p := newProducer(redisClient, cfg.StreamName)
	p.checkString()
	fmt.Println("------------------")
	p.checkSets()
	fmt.Println("------------------")
	p.checkLists()
	fmt.Println("------------------")
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFunc()
	p.pushMessages(ctx)
}

type producer struct {
	redisClient *redis.Client
	streamName  string
}

func newProducer(rCli *redis.Client, streamName string) *producer {
	return &producer{
		redisClient: rCli,
		streamName:  streamName,
	}
}

func (p *producer) checkString() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	stringValue := "some string"
	result := p.redisClient.Set(ctx, "stringKey", stringValue, 5*time.Second)
	if result.Err() != nil {
		logrus.Errorf("redis: can't set value - %s", result.Err())
		return
	}
	redisValue := p.redisClient.Get(ctx, "stringKey")
	if redisValue == nil {
		logrus.Error("redis: can't find such key")
		return
	}
	logrus.Println("test string value: ", stringValue)
	logrus.Println("string value from redis: ", redisValue.Val())
}

func (p *producer) checkHash() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	result := p.redisClient.HSet(ctx, "myHash", "keyHash", "some value")
	if result.Err() != nil {
		logrus.Errorf("redis: error while setting hash - %s", result.Err())
	}
	hash := p.redisClient.HVals(ctx, "myHash")
	logrus.Println("redis: hash for myHash - %s", hash.Val())
}

func (p *producer) checkLists() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resultLPush := p.redisClient.LPush(ctx, "testList", "value1", "value2")
	if resultLPush.Err() != nil {
		logrus.Errorf("redis: can't push values to list - %s", resultLPush.Err())
	}
	values := p.redisClient.LRange(ctx, "testList", 0, -1)
	if values.Err() != nil {
		logrus.Errorf("redis: can't get values from list - %s", values.Err())
	}
	fmt.Println("values from list: ", values.Val())
	resultRPush := p.redisClient.RPush(ctx, "testList", "value3")
	if resultRPush.Err() != nil {
		logrus.Errorf("redis: can't push values to list - %s", resultRPush.Err())
	}
	values = p.redisClient.LRange(ctx, "testList", 0, -1)
	if values.Err() != nil {
		logrus.Errorf("redis: can't get values from list - %s", values.Err())
	}
	fmt.Println("values from list: ", values.Val())
	resultLPop := p.redisClient.LPop(ctx, "testList")
	if resultLPop.Err() != nil {
		logrus.Errorf("can't delete values from list - %s", resultLPop.Err())
	}
	values = p.redisClient.LRange(ctx, "testList", 0, -1)
	if values.Err() != nil {
		logrus.Errorf("redis: can't get values from list - %s", values.Err())
	}
	fmt.Println("values from list: ", values.Val())
}

func (p *producer) checkSets() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	result := p.redisClient.SAdd(ctx, "people", "tom", "john")
	if result.Err() != nil {
		logrus.Errorf("redis: can't add values to set")
	}
	interResult := p.redisClient.SInter(ctx, "people")
	logrus.Println(interResult.Val())
}

func (p *producer) checkSortedSets() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	result := p.redisClient.ZAdd(ctx, "sortedSet", &redis.Z{
		Score:  12.5,
		Member: "Tomas",
	})
	if result.Err() != nil {
		logrus.Errorf("redis: can't add values to set - %s", result.Err())
	}
	zSetSize := p.redisClient.ZCard(ctx, "sortedSet")
	if zSetSize.Err() != nil {
		logrus.Errorf("redis: can't get zset size - %s", zSetSize.Err())
	}
	logrus.Println(zSetSize.Val())
	zRangeResult := p.redisClient.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:   "sortedSet",
		Start: 5,
		Stop:  15,
	})
	if zRangeResult.Err() != nil {
		logrus.Errorf("redis: can't get zrange - %s", zRangeResult.Err())
	}
	logrus.Println(zRangeResult.Val())
}

func (p *producer) pushMessages(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			message := "hi at " + time.Now().String()
			result := p.redisClient.XAdd(ctx, &redis.XAddArgs{
				Stream: p.streamName,
				Values: map[string]interface{}{
					"message": message,
				},
			})
			if _, err := result.Result(); err != nil {
				logrus.Errorf("producer: error while sending message - %s", err)
			}
		}
	}
}

func newRedisClient(cfg *config.Config) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisURL,
		Password: "",
		DB:       0,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("can't create new redis client instance - %s", err)
	}
	return redisClient, nil
}
