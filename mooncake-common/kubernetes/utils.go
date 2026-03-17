package main

import (
	"fmt"
	"log"
	"os"

	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const inClusterNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// NewResourceLock creates a new resource lock for use in a leader election loop.
func NewResourceLock(config *rest.Config, LeaseName, Identity string) (resourcelock.Interface, error) {
	// Default resource lock to "leases". The previous default (from v0.7.0 to v0.11.x) was configmapsleases, which was
	// used to migrate from configmaps to leases. Since the default was "configmapsleases" for over a year, spanning
	// five minor releases, any actively maintained operators are very likely to have a released version that uses
	// "configmapsleases". Therefore defaulting to "leases" should be safe.
	LeaderElectionResourceLock := resourcelock.LeasesResourceLock

	LeaseNamespace, err := GetInClusterNamespace()
	if err != nil {
		return nil, fmt.Errorf("unable to find leader election namespace: %w", err)
	}

	// Construct config for leader election
	config = rest.AddUserAgent(config, "leader-election")

	// Construct clients for leader election
	corev1Client, err := corev1client.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	coordinationClient, err := coordinationv1client.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return resourcelock.New(LeaderElectionResourceLock,
		LeaseNamespace,
		LeaseName,
		corev1Client,
		coordinationClient,
		resourcelock.ResourceLockConfig{
			Identity: Identity,
		},
	)
}

func GetInClusterNamespace() (string, error) {
	// Check whether the namespace file exists.
	// If not, we are not running in cluster so can't guess the namespace.
	if _, err := os.Stat(inClusterNamespacePath); os.IsNotExist(err) {
		return "", fmt.Errorf("not running in-cluster, please specify LeaderElectionNamespace")
	} else if err != nil {
		return "", fmt.Errorf("error checking namespace file: %w", err)
	}

	// Load the namespace file and return its content
	namespace, err := os.ReadFile(inClusterNamespacePath)
	if err != nil {
		return "", fmt.Errorf("error reading namespace file: %w", err)
	}
	return string(namespace), nil
}

// GetConfigOrDie creates a *rest.Config for talking to a Kubernetes apiserver.
// If --kubeconfig is set, will use the kubeconfig file at that location.  Otherwise will assume running
// in cluster and use the cluster provided kubeconfig.
//
// Will log an error and exit if there is an error creating the rest.Config.
func GetConfigOrDie() *rest.Config {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("unable to get kubeconfig: %v", err)
	}
	if config.QPS == 0.0 {
		config.QPS = 20.0
	}
	if config.Burst == 0 {
		config.Burst = 30
	}
	return config
}
