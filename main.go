package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

var (
	secretKey      = []byte("your-secret-key") // Ensure this is the same key used for signing tokens
	consumedTokens = make(map[string]struct{}) // Set for storing tokens
	kafkaBrokers   = []string{"localhost:9092"}
)

type TokenRequest struct {
	Username string `json:"appname"`
}

type ValidationResponse struct {
	Valid   bool                   `json:"valid"`
	Message string                 `json:"message"`
	Claims  map[string]interface{} `json:"claims,omitempty"`
}

func getKafkaProducer() sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	return producer
}

func getKafkaConsumer() sarama.Consumer {
	consumer, err := sarama.NewConsumer(kafkaBrokers, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	return consumer
}

func publishValidationResult(response ValidationResponse) {
	producer := getKafkaProducer()
	defer producer.Close()

	responseMessage, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		return
	}

	message := &sarama.ProducerMessage{
		Topic:     "token-validation",
		Value:     sarama.StringEncoder(responseMessage),
		Partition: 0,
	}

	if _, _, err := producer.SendMessage(message); err != nil {
		log.Printf("Error writing message to Kafka: %v", err)
	}
}

func CreateTokenHandler(c *gin.Context) {
	var request TokenRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"appname": request.Username,
		"exp":     time.Now().Add(time.Hour * 24).Unix(),
		"scope":   "dcr",
	})

	tokenString, err := token.SignedString(secretKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
		return
	}

	// Produce the token to Kafka
	producer := getKafkaProducer()
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: "token-creation",
		Value: sarama.StringEncoder(tokenString),
	}

	if _, _, err := producer.SendMessage(message); err != nil {
		log.Println("Error writing message:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send token to Kafka"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": tokenString})
}

func ValidateTokenHandler(c *gin.Context) {

	fmt.Println("dataaaaaaaaaaaaaaaaaa", consumedTokens)
	for key := range consumedTokens {
		fmt.Println("all key", key)
	}

	var request map[string]string
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tokenString, exists := request["token"]
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Token is required"})
		return
	}

	fmt.Println("consumed tokensssssss", consumedTokens)
	// Check if token exists in the consumedTokens map
	if _, exists := consumedTokens[tokenString]; !exists {
		response := ValidationResponse{Valid: false, Message: "Token does not exist in kafka topic,means not valid client"}
		publishValidationResult(response)
		c.JSON(http.StatusNotFound, response)
		return
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return secretKey, nil
	})

	var response ValidationResponse

	if err != nil {
		response = ValidationResponse{Valid: false, Message: "Error parsing token: " + err.Error()}
	} else if !token.Valid {
		response = ValidationResponse{Valid: false, Message: "Invalid token"}
	} else {
		// Extract and validate claims
		if claims, ok := token.Claims.(jwt.MapClaims); ok {
			// Check expiration time
			if exp, ok := claims["exp"].(float64); ok {
				expirationTime := time.Unix(int64(exp), 0)
				if time.Now().After(expirationTime) {
					response = ValidationResponse{Valid: false, Message: "Token has expired"}
				} else {
					response = ValidationResponse{
						Valid:   true,
						Message: "Valid token",
						Claims:  claims, // Include claims in the response
					}
				}
			} else {
				response = ValidationResponse{Valid: false, Message: "Token does not have an exp claim"}
			}
		} else {
			response = ValidationResponse{Valid: false, Message: "Invalid token claims"}
		}
	}

	publishValidationResult(response)
	c.JSON(http.StatusOK, response)
}

func main() {
	go func() {
		log.Println("Starting Kafka consumer...")
		consumer := getKafkaConsumer()
		defer consumer.Close()

		partitions, err := consumer.Partitions("token-creation")
		if err != nil {
			log.Fatalf("Failed to get partitions: %v", err)
		}
		takePartition := partitions[0]

		// for _, partition := range partitions {
		pc, err := consumer.ConsumePartition("token-creation", takePartition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %v", takePartition, err)
		}
		defer pc.Close()

		for msg := range pc.Messages() {
			tokenString := string(msg.Value)
			// Store the token in the map
			consumedTokens[tokenString] = struct{}{}
			log.Printf("Token consumed and stored: %s", tokenString)
		}
		// }
	}()
	// CORS middleware configuration
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true // Allow all origins for testing; adjust as needed
	corsConfig.AllowHeaders = []string{"Authorization", "Content-Type"}

	r := gin.Default()
	r.Use(cors.New(corsConfig)) // Apply CORS middleware

	r.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"hi": "hello"})
	})
	r.POST("/token", CreateTokenHandler)
	r.POST("/validate", ValidateTokenHandler)

	// Start the server
	if err := r.Run(":8080"); err != nil {
		log.Fatal("Failed to run server: ", err)
		return
	}

	fmt.Println("Server started successfully on port: 8080")
	// Keep the main function running to allow the goroutine to continue
	select {}
}
