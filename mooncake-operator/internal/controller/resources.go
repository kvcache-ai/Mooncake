package controller

import (
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	mooncakev1alpha1 "github.com/kvcache-ai/Mooncake/mooncake-operator/api/v1alpha1"
)

// parseK8sMemory converts a Kubernetes memory quantity string (e.g. "1Gi", "512Mi")
// to bytes for comparison. Returns an error if the quantity cannot be parsed.
func parseK8sMemory(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty memory quantity")
	}
	// resource.ParseQuantity handles both binary (Ki/Mi/Gi/Ti) and decimal (K/M/G/T) suffixes.
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, fmt.Errorf("invalid memory quantity %q: %w", s, err)
	}
	return q.Value(), nil
}

// validateSegmentSize ensures that segmentSize does not exceed the worker pod's
// memory limit. The mooncake_client allocates a global segment of the configured
// size inside the container, so it must fit within the container's cgroup limit.
func validateSegmentSize(mc *mooncakev1alpha1.MooncakeCluster) error {
	segBytes, err := parseK8sMemory(mc.Spec.Workers.SegmentSize)
	if err != nil {
		return fmt.Errorf("invalid segmentSize: %w", err)
	}

	// Determine the effective memory limit from the worker spec.
	// If no memory limit is set, we cannot validate — skip (Kubernetes will enforce
	// the node's allocatable if needed).
	limits := mc.Spec.Workers.Resources.Limits
	memLimit, hasLimit := limits[corev1.ResourceMemory]
	if !hasLimit || memLimit.IsZero() {
		return nil
	}

	limitBytes := memLimit.Value()
	if segBytes > limitBytes {
		return fmt.Errorf(
			"segmentSize %s (%d bytes) exceeds worker memory limit %s (%d bytes); "+
				"segmentSize must be <= worker resources.limits.memory",
			mc.Spec.Workers.SegmentSize, segBytes,
			memLimit.String(), limitBytes,
		)
	}
	return nil
}

func (r *MooncakeClusterReconciler) buildMasterStatefulSet(mc *mooncakev1alpha1.MooncakeCluster) *appsv1.StatefulSet {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-master", "cluster": mc.Name}

	image := mc.Spec.Image
	if image == "" {
		image = "mooncake/mooncake-store:latest"
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-master",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: mc.Name + "-master-headless",
			Replicas:    &mc.Spec.Master.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: mergeLabels(labels, selector),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: mc.Name,
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchLabels: selector,
									},
									TopologyKey: "kubernetes.io/hostname",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "master",
							Image:           image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command: []string{"/bin/sh", "-c"},
							Args: []string{
								fmt.Sprintf(
									`cp /etc/mooncake/master.json /tmp/master.json && `+
										`sed 's/}$/,"local_hostname":"'"${POD_IP}:%d"'"}/' `+
										`/tmp/master.json > /tmp/patched.json && `+
										`exec mooncake_master --config_path=/tmp/patched.json`,
									mc.Spec.Master.RPCPort,
								),
							},
							Env: []corev1.EnvVar{
								{
									Name:  "LD_LIBRARY_PATH",
									Value: "/usr/local/lib/mooncake",
								},
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{Name: "rpc", ContainerPort: mc.Spec.Master.RPCPort, Protocol: corev1.ProtocolTCP},
								{Name: "metrics", ContainerPort: mc.Spec.Master.MetricsPort, Protocol: corev1.ProtocolTCP},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "master-config",
									MountPath: "/etc/mooncake",
									ReadOnly:  true,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/metrics",
										Port: intstr.FromInt(int(mc.Spec.Master.MetricsPort)),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       15,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/metrics",
										Port: intstr.FromInt(int(mc.Spec.Master.MetricsPort)),
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       30,
							},
							Resources: mc.Spec.Master.Resources,
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "master-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: mc.Name + "-master-config",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// convertSegmentSize converts Kubernetes resource quantity format (e.g., "4Gi") to
// mooncake's string_to_byte_size format (e.g., "4GB"). The mooncake binary only
// supports units KB/K, MB/M, GB/G, TB/T, B - not KiB/MiB/GiB/TiB.
func convertSegmentSize(size string) string {
	size = strings.TrimSpace(size)
	// Convert KiB/MiB/GiB/TiB → KB/MB/GB/TB
	replacements := map[string]string{
		"TiB": "TB", "GiB": "GB", "MiB": "MB", "KiB": "KB",
		"Tib": "TB", "Gib": "GB", "Mib": "MB", "Kib": "KB",
		"tib": "TB", "gib": "GB", "mib": "MB", "kib": "KB",
	}
	for old, new := range replacements {
		if strings.HasSuffix(size, old) {
			return strings.TrimSuffix(size, old) + new
		}
	}
	// Convert Ki/Mi/Gi/Ti → K/M/G/T
	for _, suffix := range []string{"Ti", "Gi", "Mi", "Ki"} {
		if strings.HasSuffix(size, suffix) {
			return strings.TrimSuffix(size, suffix) + suffix[:1]
		}
	}
	return size
}

func (r *MooncakeClusterReconciler) buildWorkerDeployment(mc *mooncakev1alpha1.MooncakeCluster) *appsv1.Deployment {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-worker", "cluster": mc.Name}

	image := mc.Spec.Image
	if image == "" {
		image = "mooncake/mooncake-store:latest"
	}

	masterAddr := mc.Name + "-master-headless." + mc.Namespace + ":50051"

	transferPort := mc.Spec.Workers.TransferPort
	if transferPort == 0 {
		transferPort = 13006
	}
	portStr := strconv.Itoa(int(transferPort))

	// Failover configuration defaults
	failoverThreshold := int32(3)
	failoverRecovery := int32(30)
	if mc.Spec.Workers.TransportFailover != nil {
		if mc.Spec.Workers.TransportFailover.FailoverThreshold > 0 {
			failoverThreshold = mc.Spec.Workers.TransportFailover.FailoverThreshold
		}
		if mc.Spec.Workers.TransportFailover.RecoverySeconds > 0 {
			failoverRecovery = mc.Spec.Workers.TransportFailover.RecoverySeconds
		}
	}

	// Determine transfer protocol based on RDMA enablement
	protocol := "tcp"
	if mc.Spec.Workers.RDMAEnabled {
		protocol = "rdma"
	}

	container := corev1.Container{
		Name:            "worker",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{
			"sh",
			"-c",
			`
# Dynamically discover the leader master for metadata registration.
# In HA mode (3 master replicas), each master stores metadata independently
# in memory. Registering with a standby causes 404 errors for clients
# that query the leader. Discover the leader at startup by querying each
# master's /role endpoint.
LEADER=""
for i in 0 1 2; do
  TRY_ADDR="${CLUSTER_NAME}-master-${i}.${CLUSTER_NAME}-master-headless.${POD_NAMESPACE}.svc"
  ROLE=$(TRY_ADDR=$TRY_ADDR python3 <<- 'PYEOF' 2>/dev/null
import os, urllib.request
addr = os.environ['TRY_ADDR']
print(urllib.request.urlopen('http://' + addr + ':9003/role', timeout=3).read().decode().strip())
PYEOF
)
  if [ "${ROLE}" = "leader" ]; then
    LEADER="${TRY_ADDR}"
    break
  fi
done
[ -z "${LEADER}" ] && LEADER="${CLUSTER_NAME}-master-headless.${POD_NAMESPACE}.svc"
METADATA_SERVER="http://${LEADER}:8080/metadata"
exec mooncake_client \
  --master_server_address="` + masterAddr + `" \
  --global_segment_size="` + convertSegmentSize(mc.Spec.Workers.SegmentSize) + `" \
  --local_buffer_size=512MB \
  --metadata_server="${METADATA_SERVER}" \
  --protocol="` + protocol + `" \
  --enable_http_server \
  --http_port=9300 \
  --host="${POD_IP}:${MC_STORE_CLIENT_MIN_PORT}"
`,
		},
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"python3", "-c",
						`
import os, json, time, urllib.request, sys

pod_ip = os.environ.get('POD_IP', '')
port = os.environ.get('MC_STORE_CLIENT_MIN_PORT', '13006')
segment = pod_ip + ':' + port
cluster = os.environ.get('CLUSTER_NAME', '')
namespace = os.environ.get('POD_NAMESPACE', '')

def discover_leader():
    for i in range(3):
        try:
            addr = '%s-master-%d.%s-master-headless.%s.svc' % (cluster, i, cluster, namespace)
            r = urllib.request.urlopen('http://' + addr + ':9003/role', timeout=3)
            role = r.read().decode().strip()
            if role == 'leader':
                return addr
        except Exception:
            continue
    return '%s-master.%s.svc' % (cluster, namespace)

def get_lifecycle(leader):
    try:
        req = urllib.request.Request(
            'http://' + leader + ':9003/api/v1/segments/status?segment=' + segment)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode()
            result = json.loads(data)
            return result.get('status_name', '')
    except Exception:
        return ''

leader = discover_leader()
lifecycle = get_lifecycle(leader)
if lifecycle == 'DRAINED':
    sys.exit(0)

timeout = 600
interval = 5
elapsed = 0
while elapsed < timeout:
    leader = discover_leader()
    lifecycle = get_lifecycle(leader)
    if lifecycle == 'DRAINED':
        sys.exit(0)
    time.sleep(interval)
    elapsed += interval
sys.exit(0)
`,
					},
				},
			},
		},
		Env: []corev1.EnvVar{
			{
				Name:  "LD_LIBRARY_PATH",
				Value: "/usr/local/lib/mooncake",
			},
			{
				Name:  "CLUSTER_NAME",
				Value: mc.Name,
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			{
				Name:  "MC_STORE_CLIENT_MIN_PORT",
				Value: portStr,
			},
			{
				Name:  "MC_STORE_CLIENT_MAX_PORT",
				Value: portStr,
			},
			{
				Name:  "MC_LOCAL_BUFFER_SIZE",
				Value: "536870912",
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
			// Transport failover configuration (RDMA -> TCP)
			{
				Name:  "MC_FAILOVER_ENABLED",
				Value: fmt.Sprintf("%v", mc.Spec.Workers.TransportFailover == nil || mc.Spec.Workers.TransportFailover.Enabled),
			},
			{
				Name:  "MC_FAILOVER_THRESHOLD",
				Value: strconv.Itoa(int(failoverThreshold)),
			},
			{
				Name:  "MC_FAILOVER_RECOVERY_SECS",
				Value: strconv.Itoa(int(failoverRecovery)),
			},
		},
		Resources: mc.Spec.Workers.Resources,
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: 9300, Protocol: corev1.ProtocolTCP},
		},
	}

	volumes := []corev1.Volume{}

	// RDMA device resources
	if mc.Spec.Workers.RDMAEnabled {
		// Privileged mode required for accessing RDMA device nodes via hostPath
		container.SecurityContext = &corev1.SecurityContext{
			Privileged: boolPtr(true),
		}
		// Mount /dev/infiniband from host for RDMA device access
		volumes = append(volumes, corev1.Volume{
			Name: "rdma-dev",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/dev/infiniband",
					Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
				},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      "rdma-dev",
			MountPath: "/dev/infiniband",
		})
	}

	// GPU resources
	if mc.Spec.Workers.GPUEnabled {
		container.Resources.Limits = mergeResourceList(container.Resources.Limits, corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		})
	}

	// Hugepages
	if mc.Spec.Workers.HugepagesEnabled {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "MC_STORE_USE_HUGEPAGE",
			Value: "1",
		})
		volumes = append(volumes, corev1.Volume{
			Name: "hugepages",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium:    corev1.StorageMediumHugePages,
					SizeLimit: resourcePtr(resource.MustParse("1Gi")),
				},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      "hugepages",
			MountPath: "/dev/hugepages",
		})
	}

	replicas := mc.Spec.Workers.Replicas

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-worker",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: mergeLabels(labels, selector),
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: int64Ptr(3600),
					Containers:                    []corev1.Container{container},
					Volumes:                       volumes,
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// vLLM Proxy
// ---------------------------------------------------------------------------

func (r *MooncakeClusterReconciler) buildProxyService(mc *mooncakev1alpha1.MooncakeCluster) *corev1.Service {
	labels := labelsForCluster(mc)
	port := mc.Spec.VLLM.Proxy.Port
	if port == 0 {
		port = 8000
	}
	selector := map[string]string{"app": "mooncake-proxy", "cluster": mc.Name}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-proxy",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       port,
					TargetPort: intstr.FromInt(int(port)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: selector,
		},
	}
}

func (r *MooncakeClusterReconciler) buildProxyDeployment(mc *mooncakev1alpha1.MooncakeCluster) *appsv1.Deployment {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-proxy", "cluster": mc.Name}

	vllmImage := mc.Spec.VLLM.Image
	if vllmImage == "" {
		vllmImage = "vllm/vllm-openai:latest"
	}

	port := mc.Spec.VLLM.Proxy.Port
	if port == 0 {
		port = 8000
	}

	prefillSvc := mc.Name + "-prefill." + mc.Namespace + ".svc"
	decodeSvc := mc.Name + "-decode." + mc.Namespace + ".svc"

	replicas := mc.Spec.VLLM.Proxy.Replicas
	if replicas == 0 {
		replicas = 1
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-proxy",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: mergeLabels(labels, selector),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: mc.Name,
					Containers: []corev1.Container{
						{
							Name:            "proxy",
							Image:           vllmImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command: []string{
								"python", "-m", "mooncake.vllm_v1_proxy_server",
							},
							Args: []string{
								"--port", fmt.Sprintf("%d", port),
								"--host", "0.0.0.0",
								"--prefiller-hosts", prefillSvc,
								"--prefiller-ports", "8100",
								"--decoder-hosts", decodeSvc,
								"--decoder-ports", "8200",
							},
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: port, Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(int(port)),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(int(port)),
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       30,
							},
							Resources: mc.Spec.VLLM.Proxy.Resources,
						},
					},
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// vLLM Prefill (kv_producer)
// ---------------------------------------------------------------------------

func (r *MooncakeClusterReconciler) buildPrefillDeployment(mc *mooncakev1alpha1.MooncakeCluster) *appsv1.Deployment {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-prefill", "cluster": mc.Name}

	vllmImage := mc.Spec.VLLM.Image
	if vllmImage == "" {
		vllmImage = "vllm/vllm-openai:latest"
	}

	masterAddr := mc.Name + "-master-headless." + mc.Namespace + ":50051"

	model := mc.Spec.VLLM.Prefill.Model
	if model == "" {
		model = "Qwen/Qwen2.5-7B-Instruct"
	}

	tpSize := mc.Spec.VLLM.Prefill.TPSize
	if tpSize < 1 {
		tpSize = 1
	}

	replicas := mc.Spec.VLLM.Prefill.Replicas

	container := corev1.Container{
		Name:            "prefill",
		Image:           vllmImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{
			"vllm", "serve",
		},
		Args: []string{
			model,
			"--port", "8100",
			"--tensor-parallel-size", strconv.Itoa(int(tpSize)),
			"--kv-transfer-config", `{"kv_connector":"MooncakeConnector","kv_role":"kv_producer","kv_connector_module_path":"mooncake.mooncake_connector_v1"}`,
		},
		Env: []corev1.EnvVar{
			{
				Name:  "MOONCAKE_MASTER",
				Value: masterAddr,
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
			{
				Name:  "MOONCAKE_LOCAL_HOSTNAME",
				Value: "$(POD_IP)",
			},
		},
		Ports: []corev1.ContainerPort{
			{Name: "api", ContainerPort: 8100, Protocol: corev1.ProtocolTCP},
			{Name: "mig-http", ContainerPort: 18900, Protocol: corev1.ProtocolTCP},
		},
		Resources: mc.Spec.VLLM.Prefill.Resources,
	}

	// GPU resources for prefill
	container.Resources.Limits = mergeResourceList(container.Resources.Limits, corev1.ResourceList{
		"nvidia.com/gpu": resource.MustParse("1"),
	})

	// Migration config env vars
	if mc.Spec.VLLM.Migration.BandwidthMBPS > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "MOONCAKE_MIGRATION_BANDWIDTH_MBPS",
			Value: strconv.Itoa(int(mc.Spec.VLLM.Migration.BandwidthMBPS)),
		})
	}
	if mc.Spec.VLLM.Migration.BlockPoolSize > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "VLLM_MOONCAKE_MIGRATION_BLOCK_POOL_SIZE",
			Value: strconv.Itoa(int(mc.Spec.VLLM.Migration.BlockPoolSize)),
		})
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-prefill",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: mergeLabels(labels, selector),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: mc.Name,
					Containers:         []corev1.Container{container},
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// vLLM Decode (kv_consumer)
// ---------------------------------------------------------------------------

func (r *MooncakeClusterReconciler) buildDecodeDeployment(mc *mooncakev1alpha1.MooncakeCluster) *appsv1.Deployment {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-decode", "cluster": mc.Name}

	vllmImage := mc.Spec.VLLM.Image
	if vllmImage == "" {
		vllmImage = "vllm/vllm-openai:latest"
	}

	masterAddr := mc.Name + "-master-headless." + mc.Namespace + ":50051"

	model := mc.Spec.VLLM.Decode.Model
	if model == "" {
		model = "Qwen/Qwen2.5-7B-Instruct"
	}

	tpSize := mc.Spec.VLLM.Decode.TPSize
	if tpSize < 1 {
		tpSize = 1
	}

	replicas := mc.Spec.VLLM.Decode.Replicas

	container := corev1.Container{
		Name:            "decode",
		Image:           vllmImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{
			"vllm", "serve",
		},
		Args: []string{
			model,
			"--port", "8200",
			"--tensor-parallel-size", strconv.Itoa(int(tpSize)),
			"--kv-transfer-config", `{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer","kv_connector_module_path":"mooncake.mooncake_connector_v1"}`,
		},
		Env: []corev1.EnvVar{
			{
				Name:  "MOONCAKE_MASTER",
				Value: masterAddr,
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
			{
				Name:  "MOONCAKE_LOCAL_HOSTNAME",
				Value: "$(POD_IP)",
			},
		},
		Ports: []corev1.ContainerPort{
			{Name: "api", ContainerPort: 8200, Protocol: corev1.ProtocolTCP},
			{Name: "mig-http", ContainerPort: 18900, Protocol: corev1.ProtocolTCP},
		},
		Resources: mc.Spec.VLLM.Decode.Resources,
	}

	// GPU resources for decode
	container.Resources.Limits = mergeResourceList(container.Resources.Limits, corev1.ResourceList{
		"nvidia.com/gpu": resource.MustParse("1"),
	})

	// Migration config env vars
	if mc.Spec.VLLM.Migration.BandwidthMBPS > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "MOONCAKE_MIGRATION_BANDWIDTH_MBPS",
			Value: strconv.Itoa(int(mc.Spec.VLLM.Migration.BandwidthMBPS)),
		})
	}
	if mc.Spec.VLLM.Migration.TimeoutSeconds > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "VLLM_MOONCAKE_MIGRATION_TIMEOUT_SECONDS",
			Value: strconv.Itoa(int(mc.Spec.VLLM.Migration.TimeoutSeconds)),
		})
	}
	if mc.Spec.VLLM.Migration.BlockPoolSize > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "VLLM_MOONCAKE_MIGRATION_BLOCK_POOL_SIZE",
			Value: strconv.Itoa(int(mc.Spec.VLLM.Migration.BlockPoolSize)),
		})
	}
	if mc.Spec.VLLM.Migration.MaxRemigrationHops > 0 {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "VLLM_MOONCAKE_MIGRATION_MAX_REMIGRATION_HOPS",
			Value: strconv.Itoa(int(mc.Spec.VLLM.Migration.MaxRemigrationHops)),
		})
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-decode",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: mergeLabels(labels, selector),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: mc.Name,
					Containers:         []corev1.Container{container},
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// vLLM Prefill & Decode Services
// ---------------------------------------------------------------------------

func (r *MooncakeClusterReconciler) buildPrefillService(mc *mooncakev1alpha1.MooncakeCluster) *corev1.Service {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-prefill", "cluster": mc.Name}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-prefill",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Port:       8100,
					TargetPort: intstr.FromInt(8100),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: selector,
		},
	}
}

func (r *MooncakeClusterReconciler) buildDecodeService(mc *mooncakev1alpha1.MooncakeCluster) *corev1.Service {
	labels := labelsForCluster(mc)
	selector := map[string]string{"app": "mooncake-decode", "cluster": mc.Name}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-decode",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "api",
					Port:       8200,
					TargetPort: intstr.FromInt(8200),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: selector,
		},
	}
}

func mergeLabels(a, b map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func mergeResourceList(a, b corev1.ResourceList) corev1.ResourceList {
	result := make(corev1.ResourceList)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}

func int64Ptr(i int64) *int64 { return &i }

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType { return &t }

func boolPtr(b bool) *bool { return &b }
