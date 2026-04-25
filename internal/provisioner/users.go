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

	iterations := user.Iterations
	if iterations == 0 {
		iterations = config.MinSCRAMIterations
	}

	// AlterUserSCRAMs is idempotent — it creates or updates the user.
	// We always upsert to ensure the password is current.
	slog.Info("Ensuring SCRAM user", "username", user.Username, "mechanism", user.Mechanism, "iterations", iterations)

	upsert := kadm.UpsertSCRAM{
		User:       user.Username,
		Mechanism:  scramMechanism,
		Iterations: int32(iterations),
		Password:   user.Password,
	}

	resp, err := p.admin.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{}, []kadm.UpsertSCRAM{upsert})
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
