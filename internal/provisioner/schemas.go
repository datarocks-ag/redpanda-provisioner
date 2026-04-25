package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"redpanda-provisioner/internal/config"
)

func (p *Provisioner) ensureSchema(ctx context.Context, schema config.Schema) error {
	schemaContent, err := os.ReadFile(schema.File)
	if err != nil {
		return fmt.Errorf("reading schema file %q: %w", schema.File, err)
	}

	// Schema Registry expects the type as an uppercase token (AVRO, PROTOBUF,
	// JSON). The lowercase value has already been gated by config validation,
	// so a plain ToUpper is sufficient.
	schemaType := strings.ToUpper(schema.Type)

	slog.Info("Registering schema", "subject", schema.Subject, "type", schemaType)
	id, err := p.schema.RegisterSchema(ctx, schema.Subject, schemaType, string(schemaContent))
	if err != nil {
		return fmt.Errorf("registering schema: %w", err)
	}
	slog.Info("Schema registered", "subject", schema.Subject, "id", id)

	// Set compatibility level if specified
	if schema.Compatibility != "" {
		current, err := p.schema.GetCompatibility(ctx, schema.Subject)
		if err != nil {
			return fmt.Errorf("getting compatibility for %q: %w", schema.Subject, err)
		}

		if current != schema.Compatibility {
			slog.Info("Setting compatibility level",
				"subject", schema.Subject,
				"current", current,
				"desired", schema.Compatibility,
			)
			if err := p.schema.SetCompatibility(ctx, schema.Subject, schema.Compatibility); err != nil {
				return fmt.Errorf("setting compatibility: %w", err)
			}
		} else {
			slog.Debug("Compatibility level up to date",
				"subject", schema.Subject,
				"level", current,
			)
		}
	}

	return nil
}
