package provisioner

import (
	"context"
	"fmt"
	"log/slog"

	"redpanda-provisioner/internal/client"
	"redpanda-provisioner/internal/config"
)

// Provisioner orchestrates idempotent Redpanda resource provisioning.
type Provisioner struct {
	admin  *client.AdminClient
	schema *client.SchemaRegistryClient
	cfg    *config.Config
}

// New creates a new Provisioner.
func New(admin *client.AdminClient, schema *client.SchemaRegistryClient, cfg *config.Config) *Provisioner {
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
