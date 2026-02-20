package provisioner

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseResourceType(t *testing.T) {
	tests := []struct {
		input   string
		want    kmsg.ACLResourceType
		wantErr bool
	}{
		{"topic", kmsg.ACLResourceTypeTopic, false},
		{"group", kmsg.ACLResourceTypeGroup, false},
		{"cluster", kmsg.ACLResourceTypeCluster, false},
		{"transactional_id", kmsg.ACLResourceTypeTransactionalId, false},
		{"unknown", 0, true},
		{"", 0, true},
		{"TOPIC", 0, true}, // case sensitive
	}

	for _, tt := range tests {
		got, err := parseResourceType(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseResourceType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parseResourceType(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParsePatternType(t *testing.T) {
	tests := []struct {
		input   string
		want    kadm.ACLPattern
		wantErr bool
	}{
		{"literal", kadm.ACLPatternLiteral, false},
		{"prefixed", kadm.ACLPatternPrefixed, false},
		{"unknown", 0, true},
		{"", 0, true},
		{"LITERAL", 0, true},
	}

	for _, tt := range tests {
		got, err := parsePatternType(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parsePatternType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parsePatternType(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseOperation(t *testing.T) {
	tests := []struct {
		input   string
		want    kmsg.ACLOperation
		wantErr bool
	}{
		{"all", kmsg.ACLOperationAll, false},
		{"read", kmsg.ACLOperationRead, false},
		{"write", kmsg.ACLOperationWrite, false},
		{"create", kmsg.ACLOperationCreate, false},
		{"delete", kmsg.ACLOperationDelete, false},
		{"alter", kmsg.ACLOperationAlter, false},
		{"describe", kmsg.ACLOperationDescribe, false},
		{"cluster_action", kmsg.ACLOperationClusterAction, false},
		{"describe_configs", kmsg.ACLOperationDescribeConfigs, false},
		{"alter_configs", kmsg.ACLOperationAlterConfigs, false},
		{"idempotent_write", kmsg.ACLOperationIdempotentWrite, false},
		{"unknown", 0, true},
		{"", 0, true},
		{"READ", 0, true},
	}

	for _, tt := range tests {
		got, err := parseOperation(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseOperation(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parseOperation(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParsePermission(t *testing.T) {
	tests := []struct {
		input   string
		want    kmsg.ACLPermissionType
		wantErr bool
	}{
		{"allow", kmsg.ACLPermissionTypeAllow, false},
		{"deny", kmsg.ACLPermissionTypeDeny, false},
		{"unknown", 0, true},
		{"", 0, true},
		{"ALLOW", 0, true},
	}

	for _, tt := range tests {
		got, err := parsePermission(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parsePermission(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parsePermission(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
