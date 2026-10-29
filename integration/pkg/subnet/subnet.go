// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subnet

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"

	"github.com/gardener/gardener-gdc-ci/integration/pkg/diagnostics"
)

const (
	pollInterval = 2 * time.Second
)

// SubnetOptions defines the options for EnsureSubnet.
type SubnetOptions struct {
	Namespace         string
	Name              string
	ParentSubnetGroup string
	ParentType        string
	PrefixLength      int32
	SkipVPCLabel      bool
}

func createOrUpdateSubnet(ctx context.Context, c client.Client, opts SubnetOptions) error {
	subnet := &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.Name,
			Namespace: opts.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, subnet, func() error {
		if subnet.Annotations == nil {
			subnet.Annotations = make(map[string]string)
		}
		subnet.Annotations[ipamv1.PauseAutoDivisionAnnotation] = "true"
		if subnet.Labels == nil {
			subnet.Labels = make(map[string]string)
		}
		// Add default VPC label to satisfy admission webhook requirements unless skipped.
		// Webhook requires child subnets to have consistent VPC labels with their parent.
		// (e.g., needed for ILB tests where parent has it, but skipped for ELB tests where parent lacks it).
		if !opts.SkipVPCLabel {
			subnet.Labels[ipamv1.VPCLabel] = "default-vpc"
		}
		subnet.Spec.Type = ipamv1.Branch

		parentType := opts.ParentType
		if parentType == "" {
			parentType = string(ipamv1.SubnetGroup)
		}

		subnet.Spec.ParentReference = &ipamv1.SubnetReference{
			Name:      opts.ParentSubnetGroup,
			Namespace: ptr.To(opts.Namespace),
			Type:      ipamv1.ReferenceType(parentType),
		}
		subnet.Spec.IPv4Request = &ipamv1.SubnetRequest{PrefixLength: ptr.To(opts.PrefixLength)}
		return nil
	})
	return err
}

// EnsureSubnet creates a subnet if it doesn't exist and waits for it to be ready.
// Returns the allocated CIDR.
func EnsureSubnet(ctx context.Context, c client.Client, opts SubnetOptions, timeout time.Duration, t *testing.T) (string, error) {
	t.Logf("Creating Subnet %s in namespace %s", opts.Name, opts.Namespace)

	var lastCreateErr error
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		if err := createOrUpdateSubnet(ctx, c, opts); err != nil {
			lastCreateErr = err
			t.Logf("Failed to create or update Subnet '%s/%s' (will retry): %v", opts.Namespace, opts.Name, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		if lastCreateErr != nil {
			return "", fmt.Errorf("failed to create Subnet '%s/%s': %w (poll error: %v)", opts.Namespace, opts.Name, lastCreateErr, err)
		}
		return "", fmt.Errorf("failed to create Subnet '%s/%s': %w", opts.Namespace, opts.Name, err)
	}

	t.Log("Waiting for the subnet to be Ready")
	var allocatedCIDR string
	err = wait.PollUntilContextTimeout(
		ctx,
		pollInterval,
		timeout,
		true,
		func(ctx context.Context) (bool, error) {
			s := &ipamglobalv1.Subnet{}
			if err := c.Get(ctx, client.ObjectKey{Name: opts.Name, Namespace: opts.Namespace}, s); err != nil {
				if !apierrors.IsNotFound(err) {
					t.Logf("Failed to get Subnet '%s/%s' while waiting for Ready (will retry): %v", opts.Namespace, opts.Name, err)
				}
				return false, nil
			}
			if meta.IsStatusConditionTrue(s.Status.Conditions, "Ready") && s.Status.IPv4Allocation != nil {
				allocatedCIDR = s.Status.IPv4Allocation.CIDR
				return true, nil
			}
			return false, nil
		},
	)
	if err != nil {
		subnetYAML := diagnostics.GetObjectYAML(ctx, c, client.ObjectKey{Name: opts.Name, Namespace: opts.Namespace}, &ipamglobalv1.Subnet{})
		return "", fmt.Errorf("subnet '%s/%s' did not become Ready (timeout):\n%s\nerror: %w", opts.Namespace, opts.Name, subnetYAML, err)
	}
	t.Logf("Subnet '%s/%s' is Ready.", opts.Namespace, opts.Name)

	return allocatedCIDR, nil
}

// DeleteSubnet deletes a subnet and waits until it is gone.
func DeleteSubnet(ctx context.Context, c client.Client, key client.ObjectKey, timeout time.Duration, t *testing.T) error {
	t.Logf("Deleting Subnet %s", key.Name)
	subnet := &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}

	t.Logf("Waiting for Subnet %s to be deleted", key.Name)
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := wait.PollUntilContextCancel(pollCtx, pollInterval, true, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, key, subnet); err != nil {
			if apierrors.IsNotFound(err) {
				t.Logf("Subnet %s deleted successfully", key.Name)
				return true, nil
			}
			t.Logf("Failed to get Subnet %s during deletion (will retry): %v", key.Name, err)
			return false, nil
		}

		if subnet.DeletionTimestamp.IsZero() {
			// Retry c.Delete because the IPAM admission webhook rejects deletion
			// ("cannot delete subnet with children") while child resources (such as
			// LoadBalancer IP allocations) are still being asynchronously cleaned up.
			if err := c.Delete(ctx, subnet); client.IgnoreNotFound(err) != nil {
				t.Logf("Failed to delete subnet %s (will retry): %v", key.Name, err)
				return false, nil
			}
		}

		return false, nil
	})
	if err != nil {
		subnetYAML := diagnostics.GetObjectYAML(ctx, c, key, &ipamglobalv1.Subnet{})
		return fmt.Errorf("subnet %s not deleted (timeout):\n%s\nerror: %w", key.Name, subnetYAML, err)
	}
	return nil
}

// ValidateParentSubnetGroupCapacity checks whether the parent subnet group in namespace has
// sufficient IPv4 capacity to allocate numShoots subnets of prefixLength (/prefixLength).
// If no Subnet objects labeled with ipam.gdc.goog/subnet-group=<parentSubnetGroup> are returned,
// validation is skipped so callers without list access or pre-labeled groups are not blocked.
func ValidateParentSubnetGroupCapacity(ctx context.Context, c client.Client, namespace, parentSubnetGroup string, numShoots, prefixLength int) error {
	if c == nil || parentSubnetGroup == "" || numShoots <= 0 || prefixLength <= 0 || prefixLength > 32 {
		return nil
	}
	subnetList := &ipamglobalv1.SubnetList{}
	if err := c.List(ctx, subnetList, client.InNamespace(namespace), client.MatchingLabels{ipamv1.SubnetGroupLabel: parentSubnetGroup}); err != nil {
		return nil
	}

	var totalParentIPs int64
	minParentPrefix := 33
	for _, s := range subnetList.Items {
		if s.Status.IPv4Allocation == nil || s.Status.IPv4Allocation.CIDR == "" {
			continue
		}
		p, err := netip.ParsePrefix(s.Status.IPv4Allocation.CIDR)
		if err != nil || !p.Addr().Is4() {
			continue
		}
		if p.Bits() < minParentPrefix {
			minParentPrefix = p.Bits()
		}
		totalParentIPs += int64(1) << (32 - p.Bits())
	}
	if totalParentIPs == 0 {
		return nil
	}

	if prefixLength < minParentPrefix {
		return fmt.Errorf("requested subnet prefix /%d is larger than parent subnet group %q (/%d)", prefixLength, parentSubnetGroup, minParentPrefix)
	}

	requiredIPs := int64(numShoots) * (int64(1) << (32 - prefixLength))
	if requiredIPs > totalParentIPs {
		return fmt.Errorf("requested %d Shoot subnet(s) of /%d (%d IPs total) exceeds parent subnet group %q total capacity (%d IPs)",
			numShoots, prefixLength, requiredIPs, parentSubnetGroup, totalParentIPs)
	}
	return nil
}
