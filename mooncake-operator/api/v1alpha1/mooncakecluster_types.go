package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MooncakeClusterSpec defines the desired state of MooncakeCluster.
type MooncakeClusterSpec struct {
	// Image is the Mooncake store Docker image to deploy.
	// +kubebuilder:default="mooncake/mooncake-store:latest"
	Image string `json:"image,omitempty"`

	// Master defines the master node configuration.
	Master MasterSpec `json:"master"`

	// Workers defines the worker node configuration.
	Workers WorkerSpec `json:"workers"`

	// HA defines the high-availability backend configuration.
	// +optional
	HA HABackendSpec `json:"ha,omitempty"`

	// Snapshot defines the snapshot/restore configuration.
	// +optional
	Snapshot SnapshotSpec `json:"snapshot,omitempty"`

	// Offload defines the offload/promotion configuration.
	// +optional
	Offload OffloadSpec `json:"offload,omitempty"`

	// Eviction defines the eviction policy configuration.
	// +optional
	Eviction EvictionSpec `json:"eviction,omitempty"`
}

// MasterSpec defines the master node configuration.
type MasterSpec struct {
	// Replicas is the number of master replicas. Use 1 for non-HA, 3 for HA.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// RPCPort is the port for the RPC server.
	// +kubebuilder:default=50051
	RPCPort int32 `json:"rpcPort,omitempty"`

	// MetricsPort is the port for the metrics HTTP server.
	// +kubebuilder:default=9003
	MetricsPort int32 `json:"metricsPort,omitempty"`

	// RPCThreadNum is the number of RPC server threads.
	// +kubebuilder:default=4
	RPCThreadNum int32 `json:"rpcThreadNum,omitempty"`

	// RPCAddress is the address for the RPC server to bind to.
	// +kubebuilder:default="0.0.0.0"
	RPCAddress string `json:"rpcAddress,omitempty"`

	// RPCInterface is the network interface name for RPC. Overrides RPCAddress.
	// +optional
	RPCInterface string `json:"rpcInterface,omitempty"`

	// Resources defines the resource requirements for master pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// ConfigOverrides allows setting raw master.json configuration overrides.
	// +optional
	ConfigOverrides map[string]string `json:"configOverrides,omitempty"`

	// EnableHTTPMetadataServer enables the HTTP metadata server.
	// +optional
	EnableHTTPMetadataServer bool `json:"enableHTTPMetadataServer,omitempty"`

	// HTTPMetadataServerPort is the port for the HTTP metadata server.
	// +kubebuilder:default=8080
	HTTPMetadataServerPort int32 `json:"httpMetadataServerPort,omitempty"`
}

// WorkerSpec defines the worker node configuration.
type WorkerSpec struct {
	// Replicas is the number of worker replicas.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// SegmentSize is the memory segment size per worker (e.g. "4Gi").
	// +kubebuilder:default="4Gi"
	SegmentSize string `json:"segmentSize,omitempty"`

	// RDMAEnabled enables RDMA device access for workers.
	// +kubebuilder:default=true
	RDMAEnabled bool `json:"rdmaEnabled,omitempty"`

	// RDMPortRange is the port range for RDMA connections.
	// +kubebuilder:default="12300-14300"
	RDMPortRange string `json:"rdmaPortRange,omitempty"`

	// GPUEnabled enables GPU device access for workers.
	// +kubebuilder:default=false
	GPUEnabled bool `json:"gpuEnabled,omitempty"`

	// HugepagesEnabled enables hugepages memory allocation.
	// +kubebuilder:default=false
	HugepagesEnabled bool `json:"hugepagesEnabled,omitempty"`

	// Resources defines the resource requirements for worker pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// ConfigOverrides allows setting raw configuration overrides.
	// +optional
	ConfigOverrides map[string]string `json:"configOverrides,omitempty"`

	// MemoryAllocator is the memory allocator for global segments.
	// +kubebuilder:validation:Enum=cachelib;offset
	// +kubebuilder:default="offset"
	MemoryAllocator string `json:"memoryAllocator,omitempty"`

	// AllocationStrategy is the allocation strategy for segments.
	// +kubebuilder:validation:Enum=random;free_ratio_first;cxl
	// +kubebuilder:default="random"
	AllocationStrategy string `json:"allocationStrategy,omitempty"`
}

// HABackendSpec defines the HA backend configuration.
type HABackendSpec struct {
	// Type is the HA backend type: "k8s" (default), "etcd", "redis".
	// +kubebuilder:validation:Enum=k8s;etcd;redis
	// +kubebuilder:default="k8s"
	Type string `json:"type,omitempty"`

	// ConnectionString is the connection string for etcd/redis backends.
	// +optional
	ConnectionString string `json:"connectionString,omitempty"`

	// EtcdEndpoints is the etcd endpoints (semicolon-separated). Used when Type=etcd.
	// +optional
	EtcdEndpoints string `json:"etcdEndpoints,omitempty"`
}

// SnapshotSpec defines the snapshot/restore configuration.
type SnapshotSpec struct {
	// Enabled enables periodic snapshot of master data.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// IntervalSeconds is the interval between snapshots.
	// +kubebuilder:default=600
	IntervalSeconds int64 `json:"intervalSeconds,omitempty"`

	// ObjectStoreType is the snapshot store type: "local" or "s3".
	// +kubebuilder:validation:Enum=local;s3
	// +optional
	ObjectStoreType string `json:"objectStoreType,omitempty"`

	// BackupDir is the local directory for snapshot backup.
	// +optional
	BackupDir string `json:"backupDir,omitempty"`

	// S3Bucket is the S3 bucket for snapshot storage.
	// +optional
	S3Bucket string `json:"s3Bucket,omitempty"`

	// S3Region is the S3 region for snapshot storage.
	// +optional
	S3Region string `json:"s3Region,omitempty"`

	// RetentionCount is the number of snapshots to keep.
	// +kubebuilder:default=2
	RetentionCount int32 `json:"retentionCount,omitempty"`

	// EnableRestore enables restore from snapshot on startup.
	// +optional
	EnableRestore bool `json:"enableRestore,omitempty"`
}

// OffloadSpec defines the offload/promotion configuration.
type OffloadSpec struct {
	// Enabled enables offload availability.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// OnEvict defers offload to eviction time instead of PutEnd.
	// +optional
	OnEvict bool `json:"onEvict,omitempty"`

	// ForceEvict force-evicts objects exceeding offload cap.
	// +optional
	ForceEvict bool `json:"forceEvict,omitempty"`

	// PromotionOnHit promotes disk-only keys to memory on read access.
	// +optional
	PromotionOnHit bool `json:"promotionOnHit,omitempty"`

	// PromotionAdmissionThreshold is the min count before promotion fires.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=255
	// +kubebuilder:default=2
	PromotionAdmissionThreshold uint32 `json:"promotionAdmissionThreshold,omitempty"`

	// PromotionQueueLimit is the max in-flight promotion tasks.
	// +kubebuilder:default=50000
	PromotionQueueLimit uint32 `json:"promotionQueueLimit,omitempty"`
}

// EvictionSpec defines the eviction policy configuration.
type EvictionSpec struct {
	// Ratio is the ratio of objects to evict when memory is full.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=0.05
	Ratio float64 `json:"ratio,omitempty"`

	// HighWatermarkRatio is the ratio that triggers eviction.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=0.95
	HighWatermarkRatio float64 `json:"highWatermarkRatio,omitempty"`

	// AllowEvictSoftPinnedObjects allows eviction of soft pinned objects.
	// +kubebuilder:default=true
	AllowEvictSoftPinnedObjects bool `json:"allowEvictSoftPinnedObjects,omitempty"`

	// EnableDiskEviction enables disk eviction feature.
	// +kubebuilder:default=true
	EnableDiskEviction bool `json:"enableDiskEviction,omitempty"`
}

// MooncakeClusterStatus defines the observed state of MooncakeCluster.
type MooncakeClusterStatus struct {
	// Phase is the current lifecycle phase of the cluster.
	// +kubebuilder:validation:Enum=Creating;Running;Updating;Failed;Deleting
	Phase string `json:"phase,omitempty"`

	// MasterReady is the number of ready master replicas.
	MasterReady int32 `json:"masterReady,omitempty"`

	// WorkerReady is the number of ready worker replicas.
	WorkerReady int32 `json:"workerReady,omitempty"`

	// LeaderNode is the pod name of the current master leader.
	LeaderNode string `json:"leaderNode,omitempty"`

	// Conditions represent the latest available observations of the cluster's state.
	// +optional
	Conditions []ClusterCondition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// ClusterCondition describes the state of a MooncakeCluster at a certain point.
type ClusterCondition struct {
	// Type of the condition.
	Type string `json:"type"`

	// Status of the condition.
	Status string `json:"status"`

	// LastTransitionTime is the last time the condition transitioned.
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`

	// Reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable description.
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Master",type=integer,JSONPath=`.status.masterReady`
// +kubebuilder:printcolumn:name="Worker",type=integer,JSONPath=`.status.workerReady`
// +kubebuilder:printcolumn:name="Leader",type=string,JSONPath=`.status.leaderNode`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:resource:shortName=mc,scope=Namespaced

// MooncakeCluster is the Schema for the mooncakeclusters API.
type MooncakeCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MooncakeClusterSpec   `json:"spec,omitempty"`
	Status MooncakeClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MooncakeClusterList contains a list of MooncakeCluster.
type MooncakeClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MooncakeCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MooncakeCluster{}, &MooncakeClusterList{})
}
