package provisioner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"redpanda-provisioner/internal/config"
)

func (p *Provisioner) ensureACL(ctx context.Context, acl config.ACL) error {
	resourceType, err := parseResourceType(acl.ResourceType)
	if err != nil {
		return err
	}

	patternType, err := parsePatternType(acl.Pattern)
	if err != nil {
		return err
	}

	permissionType, err := parsePermission(acl.Permission)
	if err != nil {
		return err
	}

	for _, op := range acl.Operations {
		operation, err := parseOperation(op)
		if err != nil {
			return err
		}

		slog.Info("Ensuring ACL",
			"principal", acl.Principal,
			"operation", op,
			"resource_type", acl.ResourceType,
			"resource_name", acl.ResourceName,
			"pattern", acl.Pattern,
			"permission", acl.Permission,
		)

		// CreateACLs is idempotent in the Kafka protocol
		builder := kadm.NewACLs().
			Allow(acl.Principal).
			ResourcePatternType(patternType).
			Operations(operation)

		if permissionType == kmsg.ACLPermissionTypeDeny {
			builder = kadm.NewACLs().
				Deny(acl.Principal).
				ResourcePatternType(patternType).
				Operations(operation)
		}

		switch resourceType {
		case kmsg.ACLResourceTypeTopic:
			builder.Topics(acl.ResourceName)
		case kmsg.ACLResourceTypeGroup:
			builder.Groups(acl.ResourceName)
		case kmsg.ACLResourceTypeCluster:
			builder.Clusters()
		case kmsg.ACLResourceTypeTransactionalId:
			builder.TransactionalIDs(acl.ResourceName)
		}

		results, err := p.admin.Admin.CreateACLs(ctx, builder)
		if err != nil {
			return fmt.Errorf("creating ACL: %w", err)
		}
		for _, r := range results {
			if r.Err != nil {
				return fmt.Errorf("creating ACL for %q: %w", acl.Principal, r.Err)
			}
		}
	}

	return nil
}

func parseResourceType(s string) (kmsg.ACLResourceType, error) {
	switch s {
	case "topic":
		return kmsg.ACLResourceTypeTopic, nil
	case "group":
		return kmsg.ACLResourceTypeGroup, nil
	case "cluster":
		return kmsg.ACLResourceTypeCluster, nil
	case "transactional_id":
		return kmsg.ACLResourceTypeTransactionalId, nil
	default:
		return 0, fmt.Errorf("unknown resource type %q", s)
	}
}

func parsePatternType(s string) (kadm.ACLPattern, error) {
	switch s {
	case "literal":
		return kadm.ACLPatternLiteral, nil
	case "prefixed":
		return kadm.ACLPatternPrefixed, nil
	default:
		return 0, fmt.Errorf("unknown pattern type %q", s)
	}
}

func parseOperation(s string) (kmsg.ACLOperation, error) {
	switch s {
	case "all":
		return kmsg.ACLOperationAll, nil
	case "read":
		return kmsg.ACLOperationRead, nil
	case "write":
		return kmsg.ACLOperationWrite, nil
	case "create":
		return kmsg.ACLOperationCreate, nil
	case "delete":
		return kmsg.ACLOperationDelete, nil
	case "alter":
		return kmsg.ACLOperationAlter, nil
	case "describe":
		return kmsg.ACLOperationDescribe, nil
	case "cluster_action":
		return kmsg.ACLOperationClusterAction, nil
	case "describe_configs":
		return kmsg.ACLOperationDescribeConfigs, nil
	case "alter_configs":
		return kmsg.ACLOperationAlterConfigs, nil
	case "idempotent_write":
		return kmsg.ACLOperationIdempotentWrite, nil
	default:
		return 0, fmt.Errorf("unknown operation %q", s)
	}
}

func parsePermission(s string) (kmsg.ACLPermissionType, error) {
	switch s {
	case "allow":
		return kmsg.ACLPermissionTypeAllow, nil
	case "deny":
		return kmsg.ACLPermissionTypeDeny, nil
	default:
		return 0, fmt.Errorf("unknown permission %q", s)
	}
}
