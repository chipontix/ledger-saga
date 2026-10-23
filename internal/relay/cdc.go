package relay

import (
	"context"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdcconfig "github.com/Trendyol/go-pq-cdc-kafka/config"
	cdckafka "github.com/Trendyol/go-pq-cdc-kafka/kafka"
	pqconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/chipontix/ledger-saga/internal/failpoint"
	"github.com/chipontix/ledger-saga/internal/metrics"
)

// CDC streams outbox INSERTs from the WAL through go-pq-cdc-kafka. The
// connector acknowledges a WAL position only after the Kafka write that
// contains it succeeded, so a crash replays from the last acknowledged LSN.
type CDC struct {
	PGURL       string
	Brokers     []string
	Slot        string
	Tick        time.Duration // producer batch flush interval
	MetricsPort int           // the connector's own Prometheus endpoint
	DB          *pgxpool.Pool // for outbox retention only
	Retention   time.Duration
}

func (c *CDC) Run(ctx context.Context) error {
	u, err := url.Parse(c.PGURL)
	if err != nil {
		return err
	}
	port, _ := strconv.Atoi(u.Port())
	pass, _ := u.User.Password()
	tables := publication.Tables{
		{Name: "outbox", Schema: "saga", ReplicaIdentity: publication.ReplicaIdentityDefault},
		{Name: "outbox", Schema: "ledger", ReplicaIdentity: publication.ReplicaIdentityDefault},
	}
	cfg := cdcconfig.Connector{
		CDC: pqconfig.Config{
			Host: u.Hostname(), Port: port, Username: u.User.Username(), Password: pass,
			Database: u.Path[1:],
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              c.Slot + "_pub",
				Operations:        publication.Operations{publication.OperationInsert},
				Tables:            tables,
			},
			Slot:   slot.Config{CreateIfNotExists: true, Name: c.Slot, SlotActivityCheckerInterval: 3000},
			Metric: pqconfig.MetricConfig{Port: c.MetricsPort},
			Logger: pqconfig.LoggerConfig{LogLevel: slog.LevelWarn},
		},
		Kafka: cdcconfig.Kafka{
			// The row carries its topic; the mapping only selects the tables.
			TableTopicMapping:           map[string]string{"saga.outbox": "-", "ledger.outbox": "-"},
			Brokers:                     c.Brokers,
			RequiredAcks:                int(kafka.RequireAll),
			ProducerBatchTickerDuration: c.Tick,
		},
	}
	conn, err := cdc.NewConnector(ctx, cfg, handle, cdc.WithResponseHandler(published{}))
	if err != nil {
		return err
	}
	// Start does not return on its own when ctx ends; Close stops it.
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	go c.retain(ctx)
	conn.Start(ctx)
	return ctx.Err()
}

func handle(m *cdc.Message) []kafka.Message {
	if !m.Type.IsInsert() {
		return nil
	}
	failpoint.Hit("relay.cdc.handler")
	str := func(k string) string { s, _ := m.NewData[k].(string); return s }
	return []kafka.Message{{Topic: str("topic"), Key: []byte(str("key")), Value: []byte(str("payload")),
		Headers: []kafka.Header{{Key: "table", Value: []byte(m.TableNamespace)}}}}
}

type published struct{}

func (published) OnSuccess(*cdckafka.ResponseHandlerContext) {}
func (published) OnError(ctx *cdckafka.ResponseHandlerContext) {
	slog.Warn("cdc kafka write failed, will retry", "err", ctx.Err)
}

// OnBatchSuccess runs after the Kafka write and before the LSN ack.
func (published) OnBatchSuccess(msgs []*kafka.Message) {
	failpoint.Hit("relay.cdc.after_publish")
	for _, m := range msgs {
		metrics.Relayed.WithLabelValues("cdc", string(m.Headers[0].Value)).Inc()
	}
}

// retain deletes streamed rows. The publication carries INSERTs only, so the
// deletes are not replicated, and decoding reads the row from the WAL, not
// the table, so deleting a row the slot has not reached yet is still safe.
func (c *CDC) retain(ctx context.Context) {
	for ctx.Err() == nil {
		sleep(ctx, 10*time.Second)
		for _, schema := range []string{"saga", "ledger"} {
			if _, err := c.DB.Exec(ctx, `DELETE FROM `+schema+`.outbox WHERE created_at < now() - make_interval(secs => $1)`,
				c.Retention.Seconds()); err != nil && ctx.Err() == nil {
				slog.Warn("outbox retention", "err", err)
			}
		}
	}
}
