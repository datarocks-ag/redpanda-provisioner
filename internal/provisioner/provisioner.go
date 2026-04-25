package provisioner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"

	"redpanda-provisioner/internal/config"
)

// KafkaAdmin defines the Kafka admin operations needed by the provisioner.
type KafkaAdmin interface {
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	CreateTopics(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error)
	UpdatePartitions(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	AlterTopicConfigs(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error)
	AlterUserSCRAMs(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error)
	CreateACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error)
}

// topicCacheInvalidator is implemented by admin clients that cache topic
// metadata locally. The provisioner uses this to force a fresh broker query
// when it detects that cached state has diverged from the broker's view
// (e.g. after a stale-cache TopicAlreadyExists race).
type topicCacheInvalidator interface {
	PurgeTopicCache(topics ...string)
}

func (p *Provisioner) purgeTopicCache(topics ...string) {
	if inv, ok := p.admin.(topicCacheInvalidator); ok {
		inv.PurgeTopicCache(topics...)
	}
}

// SchemaRegistry defines the schema registry operations needed by the provisioner.
type SchemaRegistry interface {
	RegisterSchema(ctx context.Context, subject, schemaType, schema string) (int, error)
	GetCompatibility(ctx context.Context, subject string) (string, error)
	SetCompatibility(ctx context.Context, subject, level string) error
}

// Provisioner orchestrates idempotent Redpanda resource provisioning.
type Provisioner struct {
	admin  KafkaAdmin
	schema SchemaRegistry
	cfg    *config.Config
}

// New creates a new Provisioner.
func New(admin KafkaAdmin, schema SchemaRegistry, cfg *config.Config) *Provisioner {
	return &Provisioner{
		admin:  admin,
		schema: schema,
		cfg:    cfg,
	}
}

// Run executes the full provisioning sequence:
// Topics → Schemas → Users → ACLs
func (p *Provisioner) Run(ctx context.Context) error {
	slog.Info("Starting provisioning")

	for _, topic := range p.cfg.Topics {
		strategy := config.EffectiveStrategy(topic.Strategy, p.cfg.Strategy)
		if err := p.ensureTopic(ctx, topic, strategy); err != nil {
			return fmt.Errorf("provisioning topic %q: %w", topic.Name, err)
		}
	}

	for _, schema := range p.cfg.Schemas {
		if err := p.ensureSchema(ctx, schema); err != nil {
			return fmt.Errorf("provisioning schema %q: %w", schema.Subject, err)
		}
	}

	for _, user := range p.cfg.Users {
		if err := p.ensureUser(ctx, user); err != nil {
			return fmt.Errorf("provisioning user %q: %w", user.Username, err)
		}
	}

	for _, acl := range p.cfg.ACLs {
		if err := p.ensureACL(ctx, acl); err != nil {
			return fmt.Errorf("provisioning ACL for %q on %s %q: %w",
				acl.Principal, acl.ResourceType, acl.ResourceName, err)
		}
	}

	slog.Info("Provisioning complete")
	return nil
}
