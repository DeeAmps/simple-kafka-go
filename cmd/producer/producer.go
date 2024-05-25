package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/DeeAmps/kafka-notify/pkg/models"
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)


const  (
	KafkaServerAddress = "localhost:9092"
	KafkaTopic = "notifications"
)

var users = []models.User{
        {ID: 1, Name: "Emma"},
        {ID: 2, Name: "Bruno"},
        {ID: 3, Name: "Rick"},
        {ID: 4, Name: "Lena"},
    }

func getNameFromId(id int) models.User {
	for _, v := range users {
		if v.ID == id {
			return v
		}
	}
	return models.User{}
}	

func sendKafkaMessage(producer sarama.SyncProducer, fromId int, toId int, message string) error {
	notification := models.Notification{
		From: getNameFromId(fromId),
		To: getNameFromId(toId),
		Message: message,
	}

	if notification.From.Name == "" || notification.To.Name == "" {
		return errors.New("User doesnt Exist")
	}

	data, _  := json.Marshal(notification)

	messageConfig := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key: sarama.StringEncoder(strconv.Itoa(fromId)),
		Value: sarama.StringEncoder(data),
	}

	_, _, err := producer.SendMessage(messageConfig)
	return err
}

func createProducer(brokerList []string) sarama.SyncProducer {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll 
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

func sendMessageHandler(producer sarama.SyncProducer) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var json struct {
			FromID int `json:"fromId"`
			ToID int `json:"toId"`
			Message string `json:"message"`
		}

		if err := ctx.Bind(&json); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": err.Error(),
			})
			return
		}

		err := sendKafkaMessage(producer, json.FromID, json.ToID, json.Message)

		if err != nil && err.Error() == "User doesnt Exist" {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": err.Error(),
			})
			return
		}
		
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{
			"message": "notification sent",
		})


	}
}

func main() {
	r := gin.Default()

	producer := createProducer([]string{KafkaServerAddress})

	r.POST("/send", sendMessageHandler(producer))

	if err := r.Run(":8085"); err != nil {
		log.Printf("Application running on port %s", "8085")
	}
}