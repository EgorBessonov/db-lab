package main

import (
	"context"
	"github.com/EgorBessonov/db-lab/lab_3/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"time"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Fatalf("can't parse env - %s", err)
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.MongoURL))
	if err != nil {
		logrus.Fatalf("mongo: connection failed - %s", err)
	}
	mCli := NewMongoReader(client)
	rCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	mCli.ReaderOperations(rCtx)
}

type MongoReader struct {
	client *mongo.Client
}

func NewMongoReader(mCli *mongo.Client) *MongoReader {
	return &MongoReader{client: mCli}
}

func (mr *MongoReader) ReaderOperations(ctx context.Context) {
	col := mr.client.Database("test").Collection("values")
	var value int
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := col.FindOne(ctx, bson.D{{Key: "valueName", Value: "testValue"}}).Decode(&value)
			if err != nil {
				logrus.Errorf("mongo: can't get value - %s", err)
			}
			logrus.Println("current value: " + strconv.Itoa(value))
		}
	}

}
