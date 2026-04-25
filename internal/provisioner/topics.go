package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"redpanda-provisioner/internal/config"
)

func (p *Provisioner) ensureTopic(ctx context.Context, topic config.Topic, strategy string) error {
	existing, err := p.lookupTopic(ctx, topic.Name)
	if err != nil {
		return err
	}

	if existing == nil {
		createErr := p.createTopic(ctx, topic)
		if createErr == nil {
			return nil
		}
		// The franz-go metadata cache (default 5s TTL) may report a topic as
		// missing immediately after a previous run created it. If the broker
		// rejects the create as duplicate, the topic does in fact exist —
		// purge the cache and re-fetch so the update path sees real state.
		if !errors.Is(createErr, kerr.TopicAlreadyExists) {
			return createErr
		}
		slog.Info("Topic already exists; refreshing stale metadata", "topic", topic.Name)
		p.purgeTopicCache(topic.Name)
		existing, err = p.lookupTopic(ctx, topic.Name)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("topic %q reported as already-exists but not visible after metadata refresh", topic.Name)
		}
	}

	if strategy == "create" {
		slog.Info("Skipping existing topic (strategy=create)", "topic", topic.Name)
		return nil
	}

	return p.updateTopic(ctx, topic, *existing)
}

// lookupTopic returns the current topic detail, or nil if the topic does not
// exist on the broker. A topic-level UnknownTopicOrPartition error is treated
// as "does not exist" rather than an error, since that is what the broker
// returns for unknown topics.
func (p *Provisioner) lookupTopic(ctx context.Context, name string) (*kadm.TopicDetail, error) {
	topics, err := p.admin.ListTopics(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}
	detail, ok := topics[name]
	if !ok {
		return nil, nil
	}
	if detail.Err != nil {
		if errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			return nil, nil
		}
		return nil, fmt.Errorf("looking up topic %q: %w", name, detail.Err)
	}
	return &detail, nil
}

func (p *Provisioner) createTopic(ctx context.Context, topic config.Topic) error {
	partitions := int32(1)
	if topic.Partitions != nil {
		partitions = *topic.Partitions
	}

	replicationFactor := int16(1)
	if topic.ReplicationFactor != nil {
		replicationFactor = *topic.ReplicationFactor
	}

	slog.Info("Creating topic",
		"topic", topic.Name,
		"partitions", partitions,
		"replication_factor", replicationFactor,
	)

	configs := toStringPtrMap(topic.Config)
	resp, err := p.admin.CreateTopics(ctx, partitions, replicationFactor, configs, topic.Name)
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("creating topic %q: %w", r.Topic, r.Err)
		}
	}

	return nil
}

func (p *Provisioner) updateTopic(ctx context.Context, topic config.Topic, existing kadm.TopicDetail) error {
	// Check partition count — can only increase, never decrease
	currentPartitions := int32(len(existing.Partitions))

	if topic.Partitions != nil {
		desired := *topic.Partitions
		if desired < currentPartitions {
			slog.Warn("Topic has more partitions than configured (cannot decrease)",
				"topic", topic.Name,
				"current", currentPartitions,
				"desired", desired,
			)
		} else if desired > currentPartitions {
			slog.Info("Increasing topic partitions",
				"topic", topic.Name,
				"from", currentPartitions,
				"to", desired,
			)
			resp, err := p.admin.UpdatePartitions(ctx, int(desired), topic.Name)
			if err != nil {
				return fmt.Errorf("updating partitions: %w", err)
			}
			for _, r := range resp {
				if r.Err != nil {
					return fmt.Errorf("updating partitions for %q: %w", r.Topic, r.Err)
				}
			}
		}
	}

	// Update topic configs if they differ
	if len(topic.Config) == 0 {
		return nil
	}

	// Get current config to compare
	resourceCfgs, err := p.admin.DescribeTopicConfigs(ctx, topic.Name)
	if err != nil {
		return fmt.Errorf("describing topic configs: %w", err)
	}

	currentConfigs := make(map[string]string)
	for _, rc := range resourceCfgs {
		if rc.Err != nil {
			return fmt.Errorf("describing config for topic %q: %w", topic.Name, rc.Err)
		}
		for _, entry := range rc.Configs {
			if entry.Value != nil {
				currentConfigs[entry.Key] = *entry.Value
			}
		}
	}

	// Find configs that need updating
	var alters []kadm.AlterConfig
	for key, desired := range topic.Config {
		current, exists := currentConfigs[key]
		if !exists || current != desired {
			slog.Info("Updating topic config",
				"topic", topic.Name,
				"key", key,
				"current", current,
				"desired", desired,
			)
			alters = append(alters, kadm.AlterConfig{
				Op:    kadm.SetConfig,
				Name:  key,
				Value: &desired,
			})
		}
	}

	if len(alters) == 0 {
		slog.Debug("Topic config up to date", "topic", topic.Name)
		return nil
	}

	resp, err := p.admin.AlterTopicConfigs(ctx, alters, topic.Name)
	if err != nil {
		return fmt.Errorf("altering topic configs: %w", err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("altering config for topic %q: %w", r.Name, r.Err)
		}
	}

	return nil
}

func toStringPtrMap(m map[string]string) map[string]*string {
	if m == nil {
		return nil
	}
	out := make(map[string]*string, len(m))
	for k, v := range m {
		v := v
		out[k] = &v
	}
	return out
}
