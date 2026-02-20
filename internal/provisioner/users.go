package provisioner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"

	"redpanda-provisioner/internal/config"
)

func (p *Provisioner) ensureUser(ctx context.Context, user config.User) error {
	var scramMechanism kadm.ScramMechanism
	switch user.Mechanism {
	case "SCRAM-SHA-256":
		scramMechanism = kadm.ScramSha256
	case "SCRAM-SHA-512":
		scramMechanism = kadm.ScramSha512
	default:
		return fmt.Errorf("unsupported SASL mechanism %q", user.Mechanism)
	}

	// AlterUserSCRAMs is idempotent — it creates or updates the user.
	// We always upsert to ensure the password is current.
	slog.Info("Ensuring SCRAM user", "username", user.Username, "mechanism", user.Mechanism)

	upsert := kadm.UpsertSCRAM{
		User:      user.Username,
		Mechanism: scramMechanism,
		Password:  user.Password,
	}

	resp, err := p.admin.Admin.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{}, []kadm.UpsertSCRAM{upsert})
	if err != nil {
		return fmt.Errorf("upserting SCRAM user: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("upserting SCRAM user %q: %w", r.User, r.Err)
		}
	}

	slog.Info("SCRAM user ensured", "username", user.Username)
	return nil
}
