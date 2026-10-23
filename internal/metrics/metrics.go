// Package metrics holds every Prometheus series the services export.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var latency = prometheus.ExponentialBucketsRange(0.001, 60, 24)

var (
	SagaStarted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "saga_started_total", Help: "Transfers accepted by the API."})
	SagaFinished = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "saga_duration_seconds", Help: "Accept to terminal state, by terminal state.", Buckets: latency},
		[]string{"state"})
	Transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_transitions_total", Help: "State machine transitions."}, []string{"from", "to"})
	Consumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_messages_total", Help: "Messages handled, by result (applied, replayed, duplicate, stale, retried, dead_letter)."},
		[]string{"consumer", "result"})
	Delivery = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "outbox_delivery_seconds", Help: "Message creation in the producer's transaction to its consumer.", Buckets: latency},
		[]string{"consumer"})
	Relayed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_messages_total", Help: "Outbox rows written to Kafka."}, []string{"mode", "table"})
	HTTP = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "API latency.", Buckets: latency}, []string{"route", "code"})
)
