package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type UserEvent struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID string  `json:"payment_id"`
	Status    string  `json:"status"`
	Amount    float64 `json:"amount"`
}

type MovieEvent struct {
	MovieID string `json:"movie_id"`
	Action  string `json:"action"`
}

var (
	kafkaBrokers string
	kafkaTopic   = "cinemaabyss-events"
	writer       *kafka.Writer
	reader       *kafka.Reader
)

func main() {
	kafkaBrokers = os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal("KAFKA_BROKERS environment variable is required")
	}

	brokers := strings.Split(kafkaBrokers, ",")

	writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}

	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   kafkaTopic,
		GroupID: "cinemaabyss-events-group",
	})

	// Проверка подключения к Kafka
	if err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("test"),
	}); err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}

	// Запуск горутины для чтения сообщений из Kafka
	go consumeEvents()

	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":true}`))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Events service running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	var evt UserEvent
	if err := json.Unmarshal(bodyBytes, &evt); err != nil {
		http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	event := Event{Type: "User", Data: evt}
	if err := publishEvent(event); err != nil {
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "success"}`))
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	var evt MovieEvent
	if err := json.Unmarshal(bodyBytes, &evt); err != nil {
		http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	event := Event{Type: "Movie", Data: evt}
	if err := publishEvent(event); err != nil {
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "success"}`))
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	var evt PaymentEvent
	if err := json.Unmarshal(bodyBytes, &evt); err != nil {
		http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	event := Event{Type: "Payment", Data: evt}
	if err := publishEvent(event); err != nil {
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "success"}`))
}

func publishEvent(event Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(event.Type),
		Value: value,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return writer.WriteMessages(ctx, msg)
}

func consumeEvents() {
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var event Event
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Error unmarshalling event: %v", err)
			continue
		}
		// Логируем событие — здесь можно реализовать дополнительную обработку
		log.Printf("Consumed event: topic=%s partition=%d offset=%d type=%s data=%v", m.Topic, m.Partition, m.Offset, event.Type, event.Data)
	}
}
