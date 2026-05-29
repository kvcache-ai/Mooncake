package controller

import (
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	mooncakev1alpha1 "github.com/kvcache-ai/Mooncake/mooncake-operator/api/v1alpha1"
)

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
							Command: []string{
								"mooncake_master",
								"--config_path=/etc/mooncake/master.json",
							},
							Env: []corev1.EnvVar{
								{
									Name:  "LD_LIBRARY_PATH",
									Value: "/usr/local/lib/mooncake",
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
	metadataServer := "http://" + mc.Name + "-master-headless." + mc.Namespace + ":8080/metadata"

	container := corev1.Container{
		Name:            "worker",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{
			"mooncake_client",
			"--master_server_address=" + masterAddr,
			"--global_segment_size=" + convertSegmentSize(mc.Spec.Workers.SegmentSize),
			"--metadata_server=" + metadataServer,
		},
		Env: []corev1.EnvVar{
			{
				Name:  "LD_LIBRARY_PATH",
				Value: "/usr/local/lib/mooncake",
			},
		},
		Resources: mc.Spec.Workers.Resources,
	}

	// RDMA device resources
	if mc.Spec.Workers.RDMAEnabled {
		container.Resources.Limits = mergeResourceList(container.Resources.Limits, corev1.ResourceList{
			"rdma/hca_shared_devices_a": resource.MustParse("1"),
		})
	}

	// GPU resources
	if mc.Spec.Workers.GPUEnabled {
		container.Resources.Limits = mergeResourceList(container.Resources.Limits, corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		})
	}

	volumes := []corev1.Volume{}

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
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
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
