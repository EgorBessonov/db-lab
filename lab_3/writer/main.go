package main

import (
	"context"
	"github.com/EgorBessonov/db-lab/lab_3/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	wCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	mCli.SetValue(wCtx)
	mCli.WriterOperations(wCtx)
}

type MongoWriter struct {
	client *mongo.Client
}

func NewMongoReader(mCli *mongo.Client) *MongoWriter {
	return &MongoWriter{client: mCli}
}

func (mr *MongoWriter) SetValue(ctx context.Context) {
	col := mr.client.Database("test").Collection("values")
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, time.Second*5)
	defer cancelFunc()
	_, err := col.InsertOne(timeoutCtx, bson.D{
		{Key: "valueName", Value: "testValue"},
		{Key: "value", Value: "0"},
	})
	if err != nil {
		logrus.Errorf("mongo: can't insert value - %s", err)
	}
}

func (mr *MongoWriter) WriterOperations(ctx context.Context) {
	col := mr.client.Database("test").Collection("values")
	ticker := time.NewTicker(time.Second * 2)
	var value int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := col.FindOne(ctx, bson.D{{Key: "valueName", Value: "testValue"}}).Decode(&value)
			if err != nil {
				logrus.Errorf("mongo: can't get value - %s", err)
			}
			value++
			_, err = col.UpdateOne(ctx, bson.D{{Key: "valueName", Value: "testValue"}}, bson.D{
				{Key: "value", Value: value},
			})
			if err != nil {
				logrus.Errorf("mongo: can't update value - %s", err)
			}
		}
	}
}
