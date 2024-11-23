package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Database connection
var db *gorm.DB

// Kafka Writer
var kafkaWriter *kafka.Writer

// Metrics
var inboundCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "inbound_requests_total",
	Help: "Total number of inbound EDI transactions.",
})
var outboundCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "outbound_requests_total",
	Help: "Total number of outbound EDI transactions.",
})

// Transaction model for PostgreSQL
type Transaction struct {
	ID       string    `json:"id" gorm:"primaryKey"`
	Date     time.Time `json:"date"`
	ShipTo   string    `json:"ship_to"`
	ItemList string    `json:"items"` // JSON string of items
	Status   string    `json:"status"`
}

// Initialize database
func initDB() error {
	var err error
	dsn := "host=postgres user=postgres password=postgres dbname=edi_gateway port=5432 sslmode=disable"
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	return db.AutoMigrate(&Transaction{})
}

// Initialize Kafka
func initKafka() {
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"broker:9092"},
		Topic:   "edi_topic",
		BatchBytes: 200 * 1024 * 1024, // Allow larger batches
		ErrorLogger: log.New(os.Stderr, "KAFKA ERROR: ", log.LstdFlags), // Log Kafka errors
	})
}

// Handle inbound EDI
func inboundHandler(w http.ResponseWriter, r *http.Request) {
	inboundCounter.Inc()

	var transaction Transaction
	if err := json.NewDecoder(r.Body).Decode(&transaction); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Generate a unique ID for the transaction
	transaction.ID = uuid.New().String()
	transaction.Status = "Processed"
	transaction.Date = time.Now()

	// Save to PostgreSQL
	if err := db.Create(&transaction).Error; err != nil {
		log.Printf("ERROR: %v\n", err)
		http.Error(w, "Failed to save transaction", http.StatusInternalServerError)
		return
	}

	// Publish event to Kafka
	event, _ := json.Marshal(transaction)
	if err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: event}); err != nil {
		http.Error(w, "Failed to publish to Kafka", http.StatusInternalServerError)
		log.Printf("Kafka publish error: %v\n", err)
		return
	}

	fmt.Fprintf(w, "Inbound transaction processed: %+v\n", transaction)
}

// Handle outbound EDI
func outboundHandler(w http.ResponseWriter, r *http.Request) {
	outboundCounter.Inc()

	var transactions []Transaction
	if err := db.Find(&transactions).Error; err != nil {
		http.Error(w, "Failed to fetch transactions", http.StatusInternalServerError)
		return
	}

	for _, t := range transactions {
		edi := fmt.Sprintf("EDI 856: Shipment %s to %s on %s with items: %s\n",
			t.ID, t.ShipTo, t.Date.Format("2006-01-02 15:04:05"), t.ItemList)
		fmt.Fprintln(w, edi)
	}
}

// Main function
func main() {
	if err := initDB(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	initKafka()

	// Register metrics
	prometheus.MustRegister(inboundCounter, outboundCounter)

	// Setup router
	r := mux.NewRouter()
	r.HandleFunc("/inbound", inboundHandler).Methods("POST")
	r.HandleFunc("/outbound", outboundHandler).Methods("GET")
	r.Handle("/metrics", promhttp.Handler())

	log.Printf("Server running on port 8086")
	log.Fatal(http.ListenAndServe(":8086", r))
}