package provisioner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"redpanda-provisioner/internal/config"
)

// mockKafkaAdmin implements KafkaAdmin for unit tests.
type mockKafkaAdmin struct {
	listTopicsFn         func(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	createTopicsFn       func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error)
	updatePartitionsFn   func(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error)
	describeTopicCfgsFn  func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	alterTopicCfgsFn     func(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error)
	alterUserSCRAMsFn    func(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error)
	createACLsFn         func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error)
}

func (m *mockKafkaAdmin) ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
	return m.listTopicsFn(ctx, topics...)
}
func (m *mockKafkaAdmin) CreateTopics(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
	return m.createTopicsFn(ctx, partitions, replicationFactor, configs, topics...)
}
func (m *mockKafkaAdmin) UpdatePartitions(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	return m.updatePartitionsFn(ctx, partitions, topics...)
}
func (m *mockKafkaAdmin) DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
	return m.describeTopicCfgsFn(ctx, topics...)
}
func (m *mockKafkaAdmin) AlterTopicConfigs(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	return m.alterTopicCfgsFn(ctx, configs, topics...)
}
func (m *mockKafkaAdmin) AlterUserSCRAMs(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error) {
	return m.alterUserSCRAMsFn(ctx, del, upsert)
}
func (m *mockKafkaAdmin) CreateACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
	return m.createACLsFn(ctx, b)
}

// mockSchemaRegistry implements SchemaRegistry for unit tests.
type mockSchemaRegistry struct {
	registerSchemaFn   func(ctx context.Context, subject, schemaType, schema string) (int, error)
	getCompatibilityFn func(ctx context.Context, subject string) (string, error)
	setCompatibilityFn func(ctx context.Context, subject, level string) error
}

func (m *mockSchemaRegistry) RegisterSchema(ctx context.Context, subject, schemaType, schema string) (int, error) {
	return m.registerSchemaFn(ctx, subject, schemaType, schema)
}
func (m *mockSchemaRegistry) GetCompatibility(ctx context.Context, subject string) (string, error) {
	return m.getCompatibilityFn(ctx, subject)
}
func (m *mockSchemaRegistry) SetCompatibility(ctx context.Context, subject, level string) error {
	return m.setCompatibilityFn(ctx, subject, level)
}

func int32Ptr(v int32) *int32 { return &v }
func int16Ptr(v int16) *int16 { return &v }

func writeTempFile(t *testing.T, name string, content []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	return path
}

// --- Run tests ---

func TestRun_EmptyConfig(t *testing.T) {
	p := New(&mockKafkaAdmin{}, &mockSchemaRegistry{}, &config.Config{})
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRun_TopicError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return nil, errors.New("broker down")
		},
	}
	cfg := &config.Config{
		Topics: []config.Topic{{Name: "test", Partitions: int32Ptr(1)}},
	}
	p := New(admin, &mockSchemaRegistry{}, cfg)
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRun_SchemaError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{}, nil
		},
		createTopicsFn: func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
			return kadm.CreateTopicResponses{}, nil
		},
	}
	// Write a temp schema file
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			return 0, errors.New("registry down")
		},
	}
	cfg := &config.Config{
		Schemas: []config.Schema{{Subject: "test-value", Type: "avro", File: schemaFile}},
	}
	p := New(admin, schema, cfg)
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from schema registration")
	}
}

func TestRun_UserError(t *testing.T) {
	admin := &mockKafkaAdmin{
		alterUserSCRAMsFn: func(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error) {
			return nil, errors.New("auth failed")
		},
	}
	cfg := &config.Config{
		Users: []config.User{{Username: "svc", Password: "pass", Mechanism: "SCRAM-SHA-256"}},
	}
	p := New(admin, &mockSchemaRegistry{}, cfg)
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from user provisioning")
	}
}

func TestRun_ACLError(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return nil, errors.New("acl failed")
		},
	}
	cfg := &config.Config{
		ACLs: []config.ACL{{
			Principal:    "User:svc",
			Operations:   []string{"read"},
			ResourceType: "topic",
			ResourceName: "orders",
			Pattern:      "literal",
			Permission:   "allow",
		}},
	}
	p := New(admin, &mockSchemaRegistry{}, cfg)
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from ACL provisioning")
	}
}

// --- ensureTopic / createTopic / updateTopic tests ---

func TestCreateTopic_Success(t *testing.T) {
	var gotPartitions int32
	var gotRF int16
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{}, nil
		},
		createTopicsFn: func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
			gotPartitions = partitions
			gotRF = replicationFactor
			return kadm.CreateTopicResponses{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "orders", Partitions: int32Ptr(6), ReplicationFactor: int16Ptr(3)}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPartitions != 6 {
		t.Errorf("expected 6 partitions, got %d", gotPartitions)
	}
	if gotRF != 3 {
		t.Errorf("expected RF 3, got %d", gotRF)
	}
}

func TestCreateTopic_Defaults(t *testing.T) {
	var gotPartitions int32
	var gotRF int16
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{}, nil
		},
		createTopicsFn: func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
			gotPartitions = partitions
			gotRF = replicationFactor
			return kadm.CreateTopicResponses{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "orders"} // no partitions/RF set
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPartitions != 1 {
		t.Errorf("expected default 1 partition, got %d", gotPartitions)
	}
	if gotRF != 1 {
		t.Errorf("expected default RF 1, got %d", gotRF)
	}
}

func TestCreateTopic_Error(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{}, nil
		},
		createTopicsFn: func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
			return nil, errors.New("create failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureTopic(context.Background(), config.Topic{Name: "test"}, "update")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateTopic_ResponseError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{}, nil
		},
		createTopicsFn: func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
			return kadm.CreateTopicResponses{
				"test": kadm.CreateTopicResponse{Topic: "test", Err: errors.New("topic exists")},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureTopic(context.Background(), config.Topic{Name: "test"}, "update")
	if err == nil {
		t.Fatal("expected response-level error")
	}
}

func TestEnsureTopic_SkipExisting_StrategyCreate(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{Topic: "test", Partitions: kadm.PartitionDetails{0: {}}},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureTopic(context.Background(), config.Topic{Name: "test", Partitions: int32Ptr(1)}, "create")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureTopic_ListError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return nil, errors.New("list failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureTopic(context.Background(), config.Topic{Name: "test"}, "update")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUpdateTopic_PartitionIncrease(t *testing.T) {
	var newCount int
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}, 1: {}, 2: {}}, // 3 partitions
				},
			}, nil
		},
		updatePartitionsFn: func(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error) {
			newCount = partitions
			return kadm.CreatePartitionsResponses{}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Partitions: int32Ptr(6)}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newCount != 6 {
		t.Errorf("expected partition increase to 6, got %d", newCount)
	}
}

func TestUpdateTopic_PartitionDecrease_NoOp(t *testing.T) {
	updateCalled := false
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}}, // 6 partitions
				},
			}, nil
		},
		updatePartitionsFn: func(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error) {
			updateCalled = true
			return kadm.CreatePartitionsResponses{}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Partitions: int32Ptr(3)} // fewer than current
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updateCalled {
		t.Error("UpdatePartitions should not be called when decreasing")
	}
}

func TestUpdateTopic_ConfigUpdate(t *testing.T) {
	retVal := "86400000"
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{
				{
					Name: "test",
					Configs: []kadm.Config{
						{Key: "retention.ms", Value: &retVal},
					},
				},
			}, nil
		},
		alterTopicCfgsFn: func(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
			if len(configs) != 1 {
				t.Errorf("expected 1 config alteration, got %d", len(configs))
			}
			if configs[0].Name != "retention.ms" {
				t.Errorf("expected config key 'retention.ms', got %q", configs[0].Name)
			}
			return kadm.AlterConfigsResponses{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{
		Name: "test",
		Config: map[string]string{
			"retention.ms": "259200000", // different from current
		},
	}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateTopic_ConfigUpToDate(t *testing.T) {
	retVal := "259200000"
	alterCalled := false
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{
				{
					Name: "test",
					Configs: []kadm.Config{
						{Key: "retention.ms", Value: &retVal},
					},
				},
			}, nil
		},
		alterTopicCfgsFn: func(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
			alterCalled = true
			return kadm.AlterConfigsResponses{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{
		Name: "test",
		Config: map[string]string{
			"retention.ms": "259200000", // same as current
		},
	}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if alterCalled {
		t.Error("AlterTopicConfigs should not be called when configs are up to date")
	}
}

func TestUpdateTopic_DescribeConfigError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return nil, errors.New("describe failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Config: map[string]string{"retention.ms": "1000"}}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUpdateTopic_AlterConfigError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{{Name: "test"}}, nil
		},
		alterTopicCfgsFn: func(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
			return nil, errors.New("alter failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Config: map[string]string{"retention.ms": "1000"}}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUpdateTopic_UpdatePartitionsError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		updatePartitionsFn: func(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error) {
			return nil, errors.New("update failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Partitions: int32Ptr(6)}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUpdateTopic_UpdatePartitionsResponseError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		updatePartitionsFn: func(ctx context.Context, partitions int, topics ...string) (kadm.CreatePartitionsResponses, error) {
			return kadm.CreatePartitionsResponses{
				"test": kadm.CreatePartitionsResponse{Topic: "test", Err: errors.New("too many partitions")},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Partitions: int32Ptr(6)}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err == nil {
		t.Fatal("expected response-level error")
	}
}

func TestUpdateTopic_DescribeConfigResponseError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{
				{Name: "test", Err: errors.New("config error")},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Config: map[string]string{"retention.ms": "1000"}}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err == nil {
		t.Fatal("expected error from describe config response")
	}
}

func TestUpdateTopic_AlterConfigResponseError(t *testing.T) {
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			return kadm.ResourceConfigs{{Name: "test"}}, nil
		},
		alterTopicCfgsFn: func(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
			return kadm.AlterConfigsResponses{
				{Name: "test", Err: errors.New("alter config error")},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test", Config: map[string]string{"retention.ms": "1000"}}
	err := p.ensureTopic(context.Background(), topic, "update")
	if err == nil {
		t.Fatal("expected error from alter config response")
	}
}

func TestUpdateTopic_NoConfig(t *testing.T) {
	describeCalled := false
	admin := &mockKafkaAdmin{
		listTopicsFn: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test": kadm.TopicDetail{
					Topic:      "test",
					Partitions: kadm.PartitionDetails{0: {}},
				},
			}, nil
		},
		describeTopicCfgsFn: func(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
			describeCalled = true
			return kadm.ResourceConfigs{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	topic := config.Topic{Name: "test"} // no config map
	err := p.ensureTopic(context.Background(), topic, "update")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if describeCalled {
		t.Error("DescribeTopicConfigs should not be called when no config is specified")
	}
}

// --- ensureSchema tests ---

func TestEnsureSchema_RegisterAndSetCompatibility(t *testing.T) {
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	var registeredSubject, registeredType string
	var compatSet bool
	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			registeredSubject = subject
			registeredType = schemaType
			return 42, nil
		},
		getCompatibilityFn: func(ctx context.Context, subject string) (string, error) {
			return "", nil // not set yet
		},
		setCompatibilityFn: func(ctx context.Context, subject, level string) error {
			compatSet = true
			return nil
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject:       "test-value",
		Type:          "avro",
		File:          schemaFile,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if registeredSubject != "test-value" {
		t.Errorf("expected subject 'test-value', got %q", registeredSubject)
	}
	if registeredType != "AVRO" {
		t.Errorf("expected type 'AVRO', got %q", registeredType)
	}
	if !compatSet {
		t.Error("expected compatibility to be set")
	}
}

func TestEnsureSchema_CompatibilityAlreadyUpToDate(t *testing.T) {
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	setCalled := false
	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			return 1, nil
		},
		getCompatibilityFn: func(ctx context.Context, subject string) (string, error) {
			return "BACKWARD", nil
		},
		setCompatibilityFn: func(ctx context.Context, subject, level string) error {
			setCalled = true
			return nil
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject:       "test-value",
		Type:          "avro",
		File:          schemaFile,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if setCalled {
		t.Error("SetCompatibility should not be called when already up to date")
	}
}

func TestEnsureSchema_NoCompatibility(t *testing.T) {
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	getCalled := false
	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			return 1, nil
		},
		getCompatibilityFn: func(ctx context.Context, subject string) (string, error) {
			getCalled = true
			return "", nil
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject: "test-value",
		Type:    "avro",
		File:    schemaFile,
		// no Compatibility set
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if getCalled {
		t.Error("GetCompatibility should not be called when no compatibility is configured")
	}
}

func TestEnsureSchema_FileReadError(t *testing.T) {
	p := New(nil, &mockSchemaRegistry{}, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject: "test-value",
		Type:    "avro",
		File:    "/nonexistent/file.avsc",
	})
	if err == nil {
		t.Fatal("expected error for missing schema file")
	}
}

func TestEnsureSchema_RegisterError(t *testing.T) {
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			return 0, errors.New("register failed")
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject: "test-value",
		Type:    "avro",
		File:    schemaFile,
	})
	if err == nil {
		t.Fatal("expected error from schema registration")
	}
}

func TestEnsureSchema_GetCompatibilityError(t *testing.T) {
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			return 1, nil
		},
		getCompatibilityFn: func(ctx context.Context, subject string) (string, error) {
			return "", errors.New("get compat failed")
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject:       "test-value",
		Type:          "avro",
		File:          schemaFile,
		Compatibility: "BACKWARD",
	})
	if err == nil {
		t.Fatal("expected error from GetCompatibility")
	}
}

func TestEnsureSchema_SetCompatibilityError(t *testing.T) {
	schemaFile := writeTempFile(t, "test.avsc", []byte(`{"type":"string"}`))

	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			return 1, nil
		},
		getCompatibilityFn: func(ctx context.Context, subject string) (string, error) {
			return "", nil
		},
		setCompatibilityFn: func(ctx context.Context, subject, level string) error {
			return errors.New("set compat failed")
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject:       "test-value",
		Type:          "avro",
		File:          schemaFile,
		Compatibility: "BACKWARD",
	})
	if err == nil {
		t.Fatal("expected error from SetCompatibility")
	}
}

func TestEnsureSchema_ProtobufType(t *testing.T) {
	schemaFile := writeTempFile(t, "test.proto", []byte(`syntax = "proto3";`))

	var gotType string
	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			gotType = schemaType
			return 1, nil
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject: "test-value",
		Type:    "protobuf",
		File:    schemaFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotType != "PROTOBUF" {
		t.Errorf("expected type 'PROTOBUF', got %q", gotType)
	}
}

func TestEnsureSchema_JSONType(t *testing.T) {
	schemaFile := writeTempFile(t, "test.json", []byte(`{"type":"object"}`))

	var gotType string
	schema := &mockSchemaRegistry{
		registerSchemaFn: func(ctx context.Context, subject, schemaType, s string) (int, error) {
			gotType = schemaType
			return 1, nil
		},
	}
	p := New(nil, schema, &config.Config{})

	err := p.ensureSchema(context.Background(), config.Schema{
		Subject: "test-value",
		Type:    "json",
		File:    schemaFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotType != "JSON" {
		t.Errorf("expected type 'JSON', got %q", gotType)
	}
}

// --- ensureUser tests ---

func TestEnsureUser_SHA256(t *testing.T) {
	var gotMechanism kadm.ScramMechanism
	admin := &mockKafkaAdmin{
		alterUserSCRAMsFn: func(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error) {
			if len(upsert) != 1 {
				t.Fatalf("expected 1 upsert, got %d", len(upsert))
			}
			gotMechanism = upsert[0].Mechanism
			return kadm.AlteredUserSCRAMs{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureUser(context.Background(), config.User{
		Username:  "svc",
		Password:  "pass",
		Mechanism: "SCRAM-SHA-256",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMechanism != kadm.ScramSha256 {
		t.Errorf("expected ScramSha256, got %v", gotMechanism)
	}
}

func TestEnsureUser_SHA512(t *testing.T) {
	var gotMechanism kadm.ScramMechanism
	admin := &mockKafkaAdmin{
		alterUserSCRAMsFn: func(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error) {
			gotMechanism = upsert[0].Mechanism
			return kadm.AlteredUserSCRAMs{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureUser(context.Background(), config.User{
		Username:  "svc",
		Password:  "pass",
		Mechanism: "SCRAM-SHA-512",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMechanism != kadm.ScramSha512 {
		t.Errorf("expected ScramSha512, got %v", gotMechanism)
	}
}

func TestEnsureUser_UnsupportedMechanism(t *testing.T) {
	p := New(&mockKafkaAdmin{}, nil, &config.Config{})

	err := p.ensureUser(context.Background(), config.User{
		Username:  "svc",
		Password:  "pass",
		Mechanism: "PLAIN",
	})
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}

func TestEnsureUser_AlterError(t *testing.T) {
	admin := &mockKafkaAdmin{
		alterUserSCRAMsFn: func(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error) {
			return nil, errors.New("alter failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureUser(context.Background(), config.User{
		Username:  "svc",
		Password:  "pass",
		Mechanism: "SCRAM-SHA-256",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestEnsureUser_ResponseError(t *testing.T) {
	admin := &mockKafkaAdmin{
		alterUserSCRAMsFn: func(ctx context.Context, del []kadm.DeleteSCRAM, upsert []kadm.UpsertSCRAM) (kadm.AlteredUserSCRAMs, error) {
			return kadm.AlteredUserSCRAMs{
				"svc": kadm.AlteredUserSCRAM{User: "svc", Err: errors.New("user error")},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureUser(context.Background(), config.User{
		Username:  "svc",
		Password:  "pass",
		Mechanism: "SCRAM-SHA-256",
	})
	if err == nil {
		t.Fatal("expected response-level error")
	}
}

// --- ensureACL tests ---

func TestEnsureACL_AllowOnTopic(t *testing.T) {
	createCalled := false
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			createCalled = true
			return kadm.CreateACLsResults{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read", "write"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !createCalled {
		t.Error("expected CreateACLs to be called")
	}
}

func TestEnsureACL_DenyOnTopic(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return kadm.CreateACLsResults{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"write"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "deny",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureACL_GroupResource(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return kadm.CreateACLsResults{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "group",
		ResourceName: "my-group",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureACL_ClusterResource(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return kadm.CreateACLsResults{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"cluster_action"},
		ResourceType: "cluster",
		ResourceName: "kafka-cluster",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureACL_TransactionalIDResource(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return kadm.CreateACLsResults{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"write"},
		ResourceType: "transactional_id",
		ResourceName: "tx-1",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureACL_PrefixedPattern(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return kadm.CreateACLsResults{}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "topic",
		ResourceName: "orders-",
		Pattern:      "prefixed",
		Permission:   "allow",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureACL_InvalidResourceType(t *testing.T) {
	p := New(&mockKafkaAdmin{}, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "unknown",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err == nil {
		t.Fatal("expected error for invalid resource type")
	}
}

func TestEnsureACL_InvalidPattern(t *testing.T) {
	p := New(&mockKafkaAdmin{}, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "wildcard",
		Permission:   "allow",
	})
	if err == nil {
		t.Fatal("expected error for invalid pattern")
	}
}

func TestEnsureACL_InvalidPermission(t *testing.T) {
	p := New(&mockKafkaAdmin{}, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "maybe",
	})
	if err == nil {
		t.Fatal("expected error for invalid permission")
	}
}

func TestEnsureACL_InvalidOperation(t *testing.T) {
	p := New(&mockKafkaAdmin{}, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"invalid_op"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err == nil {
		t.Fatal("expected error for invalid operation")
	}
}

func TestEnsureACL_CreateError(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return nil, errors.New("create failed")
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestEnsureACL_CreateResponseError(t *testing.T) {
	admin := &mockKafkaAdmin{
		createACLsFn: func(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error) {
			return kadm.CreateACLsResults{
				{Principal: "User:svc", Err: errors.New("acl error")},
			}, nil
		},
	}
	p := New(admin, nil, &config.Config{})

	err := p.ensureACL(context.Background(), config.ACL{
		Principal:    "User:svc",
		Operations:   []string{"read"},
		ResourceType: "topic",
		ResourceName: "orders",
		Pattern:      "literal",
		Permission:   "allow",
	})
	if err == nil {
		t.Fatal("expected response-level error")
	}
}

// --- toStringPtrMap tests ---

func TestToStringPtrMap_Nil(t *testing.T) {
	result := toStringPtrMap(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestToStringPtrMap_Empty(t *testing.T) {
	result := toStringPtrMap(map[string]string{})
	if len(result) != 0 {
		t.Errorf("expected empty map, got %v", result)
	}
}

func TestToStringPtrMap_Populated(t *testing.T) {
	result := toStringPtrMap(map[string]string{"a": "1", "b": "2"})
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if *result["a"] != "1" {
		t.Errorf("expected a=1, got %q", *result["a"])
	}
	if *result["b"] != "2" {
		t.Errorf("expected b=2, got %q", *result["b"])
	}
}
