package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	mooncakev1alpha1 "github.com/kvcache-ai/Mooncake/mooncake-operator/api/v1alpha1"
)

const finalizerName = "mooncake.io/finalizer"

// MooncakeClusterReconciler reconciles a MooncakeCluster object.
type MooncakeClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=mooncake.io,resources=mooncakeclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mooncake.io,resources=mooncakeclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mooncake.io,resources=mooncakeclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *MooncakeClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch MooncakeCluster CR
	var mc mooncakev1alpha1.MooncakeCluster
	if err := r.Get(ctx, req.NamespacedName, &mc); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("MooncakeCluster not found, likely deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Handle deletion with finalizer
	if !mc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &mc)
	}

	// 3. Add finalizer if missing
	if !controllerutil.ContainsFinalizer(&mc, finalizerName) {
		patchBase := client.MergeFrom(mc.DeepCopy())
		controllerutil.AddFinalizer(&mc, finalizerName)
		if err := r.Patch(ctx, &mc, patchBase); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Set initial phase locally (no API write — reconcileStatus handles that)
	if mc.Status.Phase == "" {
		mc.Status.Phase = "Creating"
	}

	// 5. Reconcile all sub-resources
	if err := r.reconcileConfigMap(ctx, &mc); err != nil {
		return r.setFailed(ctx, &mc, "ConfigMapError", err)
	}

	if err := r.reconcileServiceAccount(ctx, &mc); err != nil {
		return r.setFailed(ctx, &mc, "ServiceAccountError", err)
	}

	if err := r.reconcileRBAC(ctx, &mc); err != nil {
		return r.setFailed(ctx, &mc, "RBACError", err)
	}

	if err := r.reconcileServices(ctx, &mc); err != nil {
		return r.setFailed(ctx, &mc, "ServiceError", err)
	}

	if err := r.reconcileMasterStatefulSet(ctx, &mc); err != nil {
		return r.setFailed(ctx, &mc, "MasterStatefulsetError", err)
	}

	if err := r.reconcileWorkerDeployment(ctx, &mc); err != nil {
		return r.setFailed(ctx, &mc, "WorkerDeploymentError", err)
	}

	// 6. Update status
	if err := r.reconcileStatus(ctx, &mc); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("reconciled MooncakeCluster", "phase", mc.Status.Phase)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *MooncakeClusterReconciler) reconcileDelete(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("cleaning up MooncakeCluster resources")

	delPatch := client.MergeFrom(mc.DeepCopy())
	mc.Status.Phase = "Deleting"
	_ = r.Status().Patch(ctx, mc, delPatch)

	// Remove finalizer
	rmPatch := client.MergeFrom(mc.DeepCopy())
	controllerutil.RemoveFinalizer(mc, finalizerName)
	if err := r.Patch(ctx, mc, rmPatch); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MooncakeClusterReconciler) setFailed(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster, reason string, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "reconciliation failed", "reason", reason)

	failedPatch := client.MergeFrom(mc.DeepCopy())
	mc.Status.Phase = "Failed"
	setCondition(&mc.Status, "Ready", "False", reason, err.Error())
	_ = r.Status().Patch(ctx, mc, failedPatch)

	r.Recorder.Event(mc, corev1.EventTypeWarning, reason, err.Error())
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *MooncakeClusterReconciler) reconcileConfigMap(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	logger := log.FromContext(ctx)
	cmName := mc.Name + "-master-config"
	desired := r.buildMasterConfigMap(mc)

	var existing corev1.ConfigMap
	err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: mc.Namespace}, &existing)
	if errors.IsNotFound(err) {
		logger.Info("ConfigMap not found, creating")
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
		logger.Info("ConfigMap created, verifying via get")
		// Re-read to verify the create actually persisted what we expect
		var created corev1.ConfigMap
		if err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: mc.Namespace}, &created); err != nil {
			logger.Info("verify get failed", "error", err)
			return nil // non-fatal
		}
		if !configMapDataEqual(created.Data, desired.Data) {
			logger.Info("created ConfigMap has wrong data, deleting and retrying with Update")
			logger.Info("desired data", "data", desired.Data)
			logger.Info("created data", "data", created.Data)
			_ = r.Delete(ctx, &created)
			existing = *desired
			existing.ResourceVersion = ""
			return r.Update(ctx, &existing)
		}
		return nil
	}
	if err != nil {
		return err
	}

	if !configMapDataEqual(existing.Data, desired.Data) {
		logger.Info("ConfigMap data differs, updating via Update")
		existing.Data = desired.Data
		return r.Update(ctx, &existing)
	}
	return nil
}

func configMapDataEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (r *MooncakeClusterReconciler) reconcileServiceAccount(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	saName := mc.Name
	desired := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: mc.Namespace,
			Labels:    labelsForCluster(mc),
		},
	}

	var existing corev1.ServiceAccount
	err := r.Get(ctx, types.NamespacedName{Name: saName, Namespace: mc.Namespace}, &existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	return err
}

func (r *MooncakeClusterReconciler) reconcileRBAC(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	roleName := mc.Name + "-lease"
	labels := labelsForCluster(mc)

	// Role for lease operations
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"coordination.k8s.io"},
				Resources: []string{"leases"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"events"},
				Verbs:     []string{"create", "patch"},
			},
		},
	}

	var existingRole rbacv1.Role
	err := r.Get(ctx, types.NamespacedName{Name: roleName, Namespace: mc.Namespace}, &existingRole)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, role); err != nil {
			return err
		}
	} else if err == nil {
		if !reflect.DeepEqual(existingRole.Rules, role.Rules) {
			patchBase := client.MergeFrom(existingRole.DeepCopy())
			existingRole.Rules = role.Rules
			if err := r.Patch(ctx, &existingRole, patchBase); err != nil {
				return err
			}
		}
	} else {
		return err
	}

	// RoleBinding
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      mc.Name,
				Namespace: mc.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     roleName,
		},
	}

	var existingRB rbacv1.RoleBinding
	err = r.Get(ctx, types.NamespacedName{Name: roleName, Namespace: mc.Namespace}, &existingRB)
	if errors.IsNotFound(err) {
		return r.Create(ctx, rb)
	}
	if err == nil {
		if !reflect.DeepEqual(existingRB.Subjects, rb.Subjects) || !reflect.DeepEqual(existingRB.RoleRef, rb.RoleRef) {
			patchBase := client.MergeFrom(existingRB.DeepCopy())
			existingRB.Subjects = rb.Subjects
			existingRB.RoleRef = rb.RoleRef
			return r.Patch(ctx, &existingRB, patchBase)
		}
		return nil
	}
	return err
}

func (r *MooncakeClusterReconciler) reconcileServices(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	labels := labelsForCluster(mc)

	// Headless service for master StatefulSet
	headlessSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-master-headless",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  map[string]string{"app": "mooncake-master", "cluster": mc.Name},
			Ports: []corev1.ServicePort{
				{Name: "rpc", Port: mc.Spec.Master.RPCPort, TargetPort: intstr.FromInt(int(mc.Spec.Master.RPCPort)), Protocol: corev1.ProtocolTCP},
				{Name: "metrics", Port: mc.Spec.Master.MetricsPort, TargetPort: intstr.FromInt(int(mc.Spec.Master.MetricsPort)), Protocol: corev1.ProtocolTCP},
				{Name: "metadata", Port: 8080, TargetPort: intstr.FromInt(8080), Protocol: corev1.ProtocolTCP},
			},
		},
	}

	if err := r.reconcileService(ctx, mc, headlessSvc); err != nil {
		return err
	}

	// Client-facing ClusterIP service
	clientSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-master",
			Namespace: mc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": "mooncake-master", "cluster": mc.Name},
			Ports: []corev1.ServicePort{
				{Name: "rpc", Port: mc.Spec.Master.RPCPort, TargetPort: intstr.FromInt(int(mc.Spec.Master.RPCPort)), Protocol: corev1.ProtocolTCP},
				{Name: "metadata", Port: 8080, TargetPort: intstr.FromInt(8080), Protocol: corev1.ProtocolTCP},
			},
		},
	}

	return r.reconcileService(ctx, mc, clientSvc)
}

func (r *MooncakeClusterReconciler) reconcileService(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster, desired *corev1.Service) error {
	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: mc.Namespace}, &existing)
	if errors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(mc, desired, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Only update if ports actually changed
	if !servicePortsEqual(existing.Spec.Ports, desired.Spec.Ports) {
		patchBase := client.MergeFrom(existing.DeepCopy())
		existing.Spec.Ports = desired.Spec.Ports
		return r.Patch(ctx, &existing, patchBase)
	}
	return nil
}

func servicePortsEqual(a, b []corev1.ServicePort) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Port != b[i].Port || a[i].Protocol != b[i].Protocol {
			return false
		}
		if a[i].TargetPort.IntVal != b[i].TargetPort.IntVal || a[i].TargetPort.StrVal != b[i].TargetPort.StrVal {
			return false
		}
		if a[i].NodePort != b[i].NodePort {
			return false
		}
	}
	return true
}

func (r *MooncakeClusterReconciler) reconcileMasterStatefulSet(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildMasterStatefulSet(mc)

	var existing appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: mc.Namespace}, &existing)
	if errors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(mc, desired, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if statefulSetSpecsEqual(&existing.Spec, &desired.Spec) {
		return nil
	}
	patchBase := client.MergeFrom(existing.DeepCopy())
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template.Spec.Containers[0].Image = desired.Spec.Template.Spec.Containers[0].Image
	existing.Spec.Template.Spec.Containers[0].ImagePullPolicy = desired.Spec.Template.Spec.Containers[0].ImagePullPolicy
	existing.Spec.Template.Spec.Containers[0].Command = desired.Spec.Template.Spec.Containers[0].Command
	existing.Spec.Template.Spec.Containers[0].Env = desired.Spec.Template.Spec.Containers[0].Env
	existing.Spec.Template.Spec.Containers[0].Resources = desired.Spec.Template.Spec.Containers[0].Resources
	existing.Spec.Template.Spec.Affinity = desired.Spec.Template.Spec.Affinity
	return r.Patch(ctx, &existing, patchBase)
}

func statefulSetSpecsEqual(a, b *appsv1.StatefulSetSpec) bool {
	if *a.Replicas != *b.Replicas {
		return false
	}
	ac := a.Template.Spec.Containers[0]
	bc := b.Template.Spec.Containers[0]
	if ac.Image != bc.Image {
		return false
	}
	if ac.ImagePullPolicy != bc.ImagePullPolicy {
		return false
	}
	if !stringSlicesEqual(ac.Command, bc.Command) {
		return false
	}
	if !envVarsEqual(ac.Env, bc.Env) {
		return false
	}
	if !reflect.DeepEqual(a.Template.Spec.Affinity, b.Template.Spec.Affinity) {
		return false
	}
	return true
}

func (r *MooncakeClusterReconciler) reconcileWorkerDeployment(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildWorkerDeployment(mc)

	var existing appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: mc.Namespace}, &existing)
	if errors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(mc, desired, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if deploymentSpecsEqual(&existing.Spec, &desired.Spec) {
		return nil
	}
	patchBase := client.MergeFrom(existing.DeepCopy())
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template.Spec.Containers[0].Image = desired.Spec.Template.Spec.Containers[0].Image
	existing.Spec.Template.Spec.Containers[0].ImagePullPolicy = desired.Spec.Template.Spec.Containers[0].ImagePullPolicy
	existing.Spec.Template.Spec.Containers[0].Command = desired.Spec.Template.Spec.Containers[0].Command
	existing.Spec.Template.Spec.Containers[0].Env = desired.Spec.Template.Spec.Containers[0].Env
	existing.Spec.Template.Spec.Containers[0].Resources = desired.Spec.Template.Spec.Containers[0].Resources
	return r.Patch(ctx, &existing, patchBase)
}

func deploymentSpecsEqual(a, b *appsv1.DeploymentSpec) bool {
	if *a.Replicas != *b.Replicas {
		return false
	}
	ac := a.Template.Spec.Containers[0]
	bc := b.Template.Spec.Containers[0]
	if ac.Image != bc.Image {
		return false
	}
	if ac.ImagePullPolicy != bc.ImagePullPolicy {
		return false
	}
	if !stringSlicesEqual(ac.Command, bc.Command) {
		return false
	}
	if !envVarsEqual(ac.Env, bc.Env) {
		return false
	}
	return true
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func envVarsEqual(a, b []corev1.EnvVar) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func (r *MooncakeClusterReconciler) reconcileStatus(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	// Snapshot current status for patch diff
	patchBase := client.MergeFrom(mc.DeepCopy())

	// Count ready master pods
	var masterPods corev1.PodList
	if err := r.List(ctx, &masterPods,
		client.InNamespace(mc.Namespace),
		client.MatchingLabels{"app": "mooncake-master", "cluster": mc.Name},
	); err != nil {
		return err
	}

	readyMasters := int32(0)
	for _, pod := range masterPods.Items {
		if isPodReady(&pod) {
			readyMasters++
		}
	}

	// Count ready worker pods
	var workerPods corev1.PodList
	if err := r.List(ctx, &workerPods,
		client.InNamespace(mc.Namespace),
		client.MatchingLabels{"app": "mooncake-worker", "cluster": mc.Name},
	); err != nil {
		return err
	}

	readyWorkers := int32(0)
	for _, pod := range workerPods.Items {
		if isPodReady(&pod) {
			readyWorkers++
		}
	}

	// Determine desired phase
	allReady := readyMasters == mc.Spec.Master.Replicas && readyWorkers == mc.Spec.Workers.Replicas
	var desiredPhase string
	if allReady {
		desiredPhase = "Running"
	} else if mc.Status.Phase != "Creating" && mc.Status.Phase != "Updating" {
		desiredPhase = "Updating"
	} else {
		desiredPhase = mc.Status.Phase
	}

	// Skip patch if nothing changed — avoids unnecessary API writes
	if mc.Status.Phase == desiredPhase &&
		mc.Status.MasterReady == readyMasters &&
		mc.Status.WorkerReady == readyWorkers &&
		mc.Status.ObservedGeneration == mc.Generation {
		return nil
	}

	mc.Status.MasterReady = readyMasters
	mc.Status.WorkerReady = readyWorkers
	mc.Status.ObservedGeneration = mc.Generation
	mc.Status.Phase = desiredPhase
	setCondition(&mc.Status, "Ready", "True",
		"PodsReady",
		fmt.Sprintf("%d/%d masters, %d/%d workers ready",
			readyMasters, mc.Spec.Master.Replicas,
			readyWorkers, mc.Spec.Workers.Replicas))

	return r.Status().Patch(ctx, mc, patchBase)
}

func (r *MooncakeClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mooncakev1alpha1.MooncakeCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func setCondition(status *mooncakev1alpha1.MooncakeClusterStatus, condType, condStatus, reason, message string) {
	now := metav1.Now()
	for i, c := range status.Conditions {
		if c.Type == condType {
			status.Conditions[i].Status = condStatus
			status.Conditions[i].LastTransitionTime = now
			status.Conditions[i].Reason = reason
			status.Conditions[i].Message = message
			return
		}
	}
	status.Conditions = append(status.Conditions, mooncakev1alpha1.ClusterCondition{
		Type:               condType,
		Status:             condStatus,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}

func (r *MooncakeClusterReconciler) buildMasterConfigMap(mc *mooncakev1alpha1.MooncakeCluster) *corev1.ConfigMap {
	config := map[string]interface{}{
		"rpc_port":                mc.Spec.Master.RPCPort,
		"rpc_thread_num":          mc.Spec.Master.RPCThreadNum,
		"rpc_address":             mc.Spec.Master.RPCAddress,
		"enable_metric_reporting": true,
		"metrics_port":            mc.Spec.Master.MetricsPort,
		"cluster_id":              mc.Namespace + "-" + mc.Name,
	}

	if mc.Spec.Master.RPCInterface != "" {
		config["rpc_interface"] = mc.Spec.Master.RPCInterface
	}

	if mc.Spec.Master.EnableHTTPMetadataServer {
		config["enable_http_metadata_server"] = true
		config["http_metadata_server_port"] = mc.Spec.Master.HTTPMetadataServerPort
	} else {
		// Always enable HTTP metadata server for client discovery
		config["enable_http_metadata_server"] = true
		config["http_metadata_server_port"] = 8080
	}

	// HA config
	if mc.Spec.HA.Type != "" {
		config["enable_ha"] = true
		config["ha_backend_type"] = mc.Spec.HA.Type
		if mc.Spec.HA.ConnectionString != "" {
			config["ha_backend_connstring"] = mc.Spec.HA.ConnectionString
		} else if mc.Spec.HA.Type == "k8s" {
			// For k8s backend, auto-generate connstring as namespace/lease-name
			config["ha_backend_connstring"] = mc.Namespace + "/" + mc.Name + "-leader-lease"
		}
		if mc.Spec.HA.EtcdEndpoints != "" {
			config["etcd_endpoints"] = mc.Spec.HA.EtcdEndpoints
		}
	}

	// Eviction config
	config["eviction_ratio"] = mc.Spec.Eviction.Ratio
	config["eviction_high_watermark_ratio"] = mc.Spec.Eviction.HighWatermarkRatio
	config["allow_evict_soft_pinned_objects"] = mc.Spec.Eviction.AllowEvictSoftPinnedObjects
	config["enable_disk_eviction"] = mc.Spec.Eviction.EnableDiskEviction

	// Snapshot config
	if mc.Spec.Snapshot.Enabled {
		config["enable_snapshot"] = true
		config["snapshot_interval_seconds"] = mc.Spec.Snapshot.IntervalSeconds
		config["snapshot_object_store_type"] = mc.Spec.Snapshot.ObjectStoreType
		config["snapshot_backup_dir"] = mc.Spec.Snapshot.BackupDir
		config["snapshot_s3_bucket"] = mc.Spec.Snapshot.S3Bucket
		config["snapshot_s3_region"] = mc.Spec.Snapshot.S3Region
		config["snapshot_retention_count"] = mc.Spec.Snapshot.RetentionCount
		config["enable_restore"] = mc.Spec.Snapshot.EnableRestore
	}

	// Offload config
	if mc.Spec.Offload.Enabled {
		config["enable_offload"] = true
		config["offload_on_evict"] = mc.Spec.Offload.OnEvict
		config["offload_force_evict"] = mc.Spec.Offload.ForceEvict
		config["promotion_on_hit"] = mc.Spec.Offload.PromotionOnHit
		config["promotion_admission_threshold"] = mc.Spec.Offload.PromotionAdmissionThreshold
		config["promotion_queue_limit"] = mc.Spec.Offload.PromotionQueueLimit
	}

	// Apply overrides (with type conversion from string)
	for k, v := range mc.Spec.Master.ConfigOverrides {
		if v == "true" || v == "false" {
			config[k] = v == "true"
		} else if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			config[k] = n
		} else if f, err := strconv.ParseFloat(v, 64); err == nil {
			config[k] = f
		} else {
			config[k] = v
		}
	}

	data, _ := json.MarshalIndent(config, "", "  ")

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mc.Name + "-master-config",
			Namespace: mc.Namespace,
			Labels:    labelsForCluster(mc),
		},
		Data: map[string]string{
			"master.json": string(data),
		},
	}
}

func labelsForCluster(mc *mooncakev1alpha1.MooncakeCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "mooncake",
		"app.kubernetes.io/instance":   mc.Name,
		"app.kubernetes.io/managed-by": "mooncake-operator",
	}
}
