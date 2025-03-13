package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/apache/pulsar-client-go/pulsar"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/grafana/pyroscope-go"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	_ "go.uber.org/automaxprocs"
)

var (
	pulsarProducers  = &sync.Map{}
	pulsarClient     pulsar.Client
	client           mqtt.Client
	profiler         *pyroscope.Profiler
	messagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messages_produced",
			Help: "Number of messages produced to Pulsar",
		},
		[]string{"topic"},
	)
)

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		if os.IsNotExist(err) {
			log.Println("No .env file found, using environment variables directly.")
		} else {
			log.Printf("Error loading .env file: %v\n", err)
		}
	}

	var profError error
	profiler, profError = setupProfiling()
	if profError != nil {
		log.Fatal(profError)
	}

	// Connect to MQTT Broker
	opts := mqtt.NewClientOptions()
	opts.AddBroker(os.Getenv("MQTT_BROKER_URL"))
	opts.ClientID = os.Getenv("MQTT_CLIENT_ID")
	opts.Password = os.Getenv("MQTT_PASSWORD")
	opts.Username = os.Getenv("MQTT_USERNAME")
	client = mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	log.Println("Connected to mqtt")

	// Connect to Pulsar
	var errPulsar error
	pulsarClient, errPulsar = pulsar.NewClient(pulsar.ClientOptions{
		URL:          os.Getenv("PULSAR_BROKER_URL"),
		ListenerName: "internal",
	})
	if errPulsar != nil {
		log.Fatal(errPulsar)
	}
	defer pulsarClient.Close()

	log.Println("Connected to pulsar")

	// Start Prometheus metrics endpoint
	go func() {
		port := os.Getenv("PROMETHEUS_PORT")
		log.Printf("Starting Prometheus metrics at http://localhost:%s/metrics\n", port)
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(fmt.Sprintf(":%s", port), nil); err != nil {
			log.Fatal(err)
		}
	}()

	// Subscribe to MQTT topics with wildcard
	subscribeToMQTT(client)

	// Capture SIGINT and SIGTERM signals
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Wait for termination signal
	<-ctx.Done()

	// Begin shutdown process
	log.Println("Received shutdown signal, starting graceful shutdown...")
	shutdown()
}

func subscribeToMQTT(client mqtt.Client) {
	token := client.Subscribe("device/#", 0, func(client mqtt.Client, msg mqtt.Message) {
		handleMQTTMessage(msg)
	})
	if token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
}

func handleMQTTMessage(msg mqtt.Message) {
	ctx := context.Background()
	tracer := otel.GetTracerProvider().Tracer("mqtt-to-pulsar")
	ctx, span := tracer.Start(ctx, "produce-to-pulsar")
	defer span.End()

	// Extract MQTT topic
	mqttTopic := msg.Topic()

	// Map MQTT topic to Pulsar topic using wildcard logic
	pulsarTopic := mapMQTTToPulsarTopic(mqttTopic)

	// Get or create Pulsar producer for the topic
	producer, ok := getOrCreateProducer(pulsarTopic)
	if !ok {
		log.Printf("Failed to get or create producer for topic: %s\n", pulsarTopic)
		return
	}

	pmsg := &pulsar.ProducerMessage{
		Payload: msg.Payload(),
	}

	if _, err := producer.Send(ctx, pmsg); err != nil {
		log.Println(err)
	}

	log.Println("Message Processed")

	// Increment Prometheus metric
	messagesProduced.With(prometheus.Labels{"topic": pulsarTopic}).Inc()
}

func getOrCreateProducer(topic string) (pulsar.Producer, bool) {
	value, ok := pulsarProducers.Load(topic)
	if ok {
		return value.(pulsar.Producer), true
	}

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Printf("Failed to create producer for topic: %s, error: %v\n", topic, err)
		return nil, false
	}

	pulsarProducers.Store(topic, producer)
	return producer, true
}

func mapMQTTToPulsarTopic(mqttTopic string) string {
	parts := strings.Split(mqttTopic, "/")
	return fmt.Sprintf("persistent://public/default/%s", strings.Join(parts[1:], "/"))
}

func shutdown() {
	// Close all Pulsar producers
	pulsarProducers.Range(func(key, value any) bool {
		producer := value.(pulsar.Producer)
		producer.Close()
		return true
	})

	// Disconnect from MQTT broker
	client.Disconnect(250)
	// Close Pulsar client
	pulsarClient.Close()

	profiler.Flush(false)
	err := profiler.Stop()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Graceful shutdown completed.")
}

func setupProfiling() (*pyroscope.Profiler, error) {
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)
	// Start Pyroscope with configuration
	return pyroscope.Start(pyroscope.Config{
		ApplicationName: "mqtt-to-pulsar",
		ServerAddress:   os.Getenv("PULSAR_URL"), // Replace with your server address
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
}
