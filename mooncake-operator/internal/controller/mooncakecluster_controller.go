package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"sync"
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
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;patch;delete
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

	// 6. Reconcile vLLM components (proxy, prefill, decode)
	if mc.Spec.VLLM != nil {
		if result, err := r.reconcileVLLM(ctx, &mc); err != nil {
			return result, err
		}
	}

	// 7. Update status
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
				{Name: "metrics", Port: mc.Spec.Master.MetricsPort, TargetPort: intstr.FromInt(int(mc.Spec.Master.MetricsPort)), Protocol: corev1.ProtocolTCP},
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

	// Scale-down detection: orchestrate data migration before reducing replicas
	if *desired.Spec.Replicas < *existing.Spec.Replicas {
		terminateCount := int(*existing.Spec.Replicas - *desired.Spec.Replicas)
		if err := r.orchestrateWorkerScaleDown(ctx, mc, terminateCount); err != nil {
			r.Recorder.Event(mc, corev1.EventTypeWarning, "WorkerMigrationError",
				fmt.Sprintf("Worker data migration failed: %v", err))
		}
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

func (r *MooncakeClusterReconciler) orchestrateWorkerScaleDown(
	ctx context.Context,
	mc *mooncakev1alpha1.MooncakeCluster,
	terminateCount int,
) error {
	logger := log.FromContext(ctx)
	logger.Info("worker scale-down detected, draining data before termination",
		"terminateCount", terminateCount)

	// List all worker pods, sorted by creation timestamp descending (newest first)
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(mc.Namespace),
		client.MatchingLabels{"app": "mooncake-worker", "cluster": mc.Name},
	); err != nil {
		return fmt.Errorf("listing worker pods: %w", err)
	}

	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.After(pods.Items[j].CreationTimestamp.Time)
	})

	if len(pods.Items) < terminateCount {
		return fmt.Errorf("not enough worker pods: need %d, have %d", terminateCount, len(pods.Items))
	}

	terminatingPods := pods.Items[:terminateCount]
	logger.Info("terminating worker pods", "count", len(terminatingPods))

	// Build master HTTP address using ClusterIP service (stable DNS)
	masterAddr := fmt.Sprintf("http://%s-master.%s.svc:%d",
		mc.Name, mc.Namespace, mc.Spec.Master.MetricsPort)

	// Read migration config from CRD
	maxConcurrency := int32(4)
	bandwidthMBPS := int32(0)
	timeoutSeconds := int32(300)
	if mc.Spec.Workers.Migration != nil {
		if mc.Spec.Workers.Migration.MaxConcurrency > 0 {
			maxConcurrency = mc.Spec.Workers.Migration.MaxConcurrency
		}
		if mc.Spec.Workers.Migration.TimeoutSeconds > 0 {
			timeoutSeconds = mc.Spec.Workers.Migration.TimeoutSeconds
		}
		bandwidthMBPS = mc.Spec.Workers.Migration.BandwidthMBPS
	}

	transferPort := mc.Spec.Workers.TransferPort
	if transferPort == 0 {
		transferPort = 13006
	}

	// Create drain jobs concurrently
	type jobResult struct {
		podName string
		podIP   string
		jobID   string
		err     error
	}

	var wg sync.WaitGroup
	results := make(chan jobResult, len(terminatingPods))

	for _, pod := range terminatingPods {
		pod := pod
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobID, err := r.drainWorker(ctx, masterAddr, pod.Status.PodIP, transferPort, maxConcurrency, bandwidthMBPS)
			results <- jobResult{podName: pod.Name, podIP: pod.Status.PodIP, jobID: jobID, err: err}
		}()
	}

	wg.Wait()
	close(results)

	var jobIDs []string
	for result := range results {
		if result.err != nil {
			logger.Error(result.err, "drain job creation failed", "pod", result.podName)
			r.Recorder.Event(mc, corev1.EventTypeWarning, "DrainJobCreationFailed",
				fmt.Sprintf("drain job failed for pod %s (IP: %s): %v", result.podName, result.podIP, result.err))
			continue
		}
		jobIDs = append(jobIDs, result.jobID)
		logger.Info("drain job created", "pod", result.podName, "jobID", result.jobID)
		r.Recorder.Event(mc, corev1.EventTypeNormal, "DrainJobCreated",
			fmt.Sprintf("drain job %s created for pod %s", result.jobID, result.podName))
	}

	if len(jobIDs) == 0 {
		return fmt.Errorf("all drain job creations failed")
	}

	// Wait for all drain jobs to complete
	return r.waitDrainJobs(ctx, masterAddr, jobIDs, time.Duration(timeoutSeconds)*time.Second)
}

// drainWorker creates a drain job on the master for a single worker.
// The segment name is POD_IP:TransferPort (e.g., "10.244.2.5:13006").
func (r *MooncakeClusterReconciler) drainWorker(
	ctx context.Context,
	masterAddr string,
	podIP string,
	transferPort int32,
	maxConcurrency int32,
	bandwidthMBPS int32,
) (string, error) {
	segmentName := fmt.Sprintf("%s:%d", podIP, transferPort)
	reqBody := map[string]interface{}{
		"segments":        []string{segmentName},
		"max_concurrency": maxConcurrency,
		"bandwidth_mbps":  bandwidthMBPS,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling drain request: %w", err)
	}

	url := masterAddr + "/api/v1/drain_jobs"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("creating drain request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("posting drain job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("drain job request failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var createResp struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		// Try reading raw body
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("decoding drain response (body: %s): %w", string(respBody), err)
	}

	if createResp.ID == "" {
		return "", fmt.Errorf("drain job response missing id")
	}

	return createResp.ID, nil
}

// waitDrainJobs polls drain job statuses until all complete or timeout.
func (r *MooncakeClusterReconciler) waitDrainJobs(
	ctx context.Context,
	masterAddr string,
	jobIDs []string,
	timeout time.Duration,
) error {
	logger := log.FromContext(ctx)
	deadline := time.Now().Add(timeout)
	httpClient := &http.Client{Timeout: 5 * time.Second}

	done := make(map[string]bool)
	for _, id := range jobIDs {
		done[id] = false
	}

	pollInterval := 2 * time.Second
	for {
		if time.Now().After(deadline) {
			var pendingIDs []string
			for id, completed := range done {
				if !completed {
					pendingIDs = append(pendingIDs, id)
				}
			}
			return fmt.Errorf("drain jobs timed out after %v: %v", timeout, pendingIDs)
		}

		for id, completed := range done {
			if completed {
				continue
			}

			url := fmt.Sprintf("%s/api/v1/drain_jobs/query?job_id=%s", masterAddr, id)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				logger.Error(err, "creating drain query request", "jobID", id)
				continue
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				logger.Error(err, "querying drain job", "jobID", id)
				continue
			}

			var queryResp struct {
				ID             string `json:"id"`
				Status         int    `json:"status"`
				SucceededUnits uint64 `json:"succeeded_units"`
				FailedUnits    uint64 `json:"failed_units"`
				MigratedBytes  uint64 `json:"migrated_bytes"`
				Message        string `json:"message"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
				resp.Body.Close()
				logger.Error(err, "decoding drain query response", "jobID", id)
				continue
			}
			resp.Body.Close()

			// Status values: 0=CREATED, 1=PLANNING, 2=RUNNING, 3=SUCCEEDED, 4=FAILED, 5=CANCELED
			switch queryResp.Status {
			case 3: // SUCCEEDED
				done[id] = true
				logger.Info("drain job succeeded", "jobID", id,
					"migratedBytes", queryResp.MigratedBytes,
					"succeededUnits", queryResp.SucceededUnits)
			case 4: // FAILED
				done[id] = true
				logger.Info("drain job failed", "jobID", id,
					"message", queryResp.Message,
					"failedUnits", queryResp.FailedUnits)
			case 5: // CANCELED
				done[id] = true
				logger.Info("drain job canceled", "jobID", id)
			default:
				// Still running, log progress
				if queryResp.MigratedBytes > 0 || queryResp.SucceededUnits > 0 {
					logger.V(1).Info("drain job in progress", "jobID", id,
						"status", queryResp.Status,
						"migratedBytes", queryResp.MigratedBytes,
						"succeededUnits", queryResp.SucceededUnits)
				}
			}

		}

		if allDoneExcept(done) {
			return nil
		}

		time.Sleep(pollInterval)
	}
}

// allDoneExcept returns true if all tracked items are done
func allDoneExcept(done map[string]bool) bool {
	for _, completed := range done {
		if !completed {
			return false
		}
	}
	return true
}

func (r *MooncakeClusterReconciler) patchDeploymentSpec(ctx context.Context, existing *appsv1.Deployment, desired *appsv1.Deployment) error {
	patchBase := client.MergeFrom(existing.DeepCopy())
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template.Spec.Containers[0].Image = desired.Spec.Template.Spec.Containers[0].Image
	existing.Spec.Template.Spec.Containers[0].ImagePullPolicy = desired.Spec.Template.Spec.Containers[0].ImagePullPolicy
	existing.Spec.Template.Spec.Containers[0].Command = desired.Spec.Template.Spec.Containers[0].Command
	existing.Spec.Template.Spec.Containers[0].Args = desired.Spec.Template.Spec.Containers[0].Args
	existing.Spec.Template.Spec.Containers[0].Env = desired.Spec.Template.Spec.Containers[0].Env
	existing.Spec.Template.Spec.Containers[0].Resources = desired.Spec.Template.Spec.Containers[0].Resources
	existing.Spec.Template.Spec.Containers[0].Ports = desired.Spec.Template.Spec.Containers[0].Ports
	return r.Patch(ctx, existing, patchBase)
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

	// Count ready vLLM pods (only if VLLM is configured)
	readyProxies := int32(0)
	readyPrefills := int32(0)
	readyDecodes := int32(0)
	vllmConfigured := mc.Spec.VLLM != nil
	if vllmConfigured {
		var vllmPods corev1.PodList
		if err := r.List(ctx, &vllmPods,
			client.InNamespace(mc.Namespace),
			client.MatchingLabels{"cluster": mc.Name},
		); err != nil {
			return err
		}

		for _, pod := range vllmPods.Items {
			if !isPodReady(&pod) {
				continue
			}
			appLabel := pod.Labels["app"]
			switch appLabel {
			case "mooncake-proxy":
				readyProxies++
			case "mooncake-prefill":
				readyPrefills++
			case "mooncake-decode":
				readyDecodes++
			}
		}
	}

	// Determine desired phase
	allReady := readyMasters == mc.Spec.Master.Replicas && readyWorkers == mc.Spec.Workers.Replicas
	if vllmConfigured {
		allReady = allReady &&
			readyProxies == mc.Spec.VLLM.Proxy.Replicas &&
			readyPrefills == mc.Spec.VLLM.Prefill.Replicas &&
			readyDecodes == mc.Spec.VLLM.Decode.Replicas
	}
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
		(!vllmConfigured ||
			(mc.Status.ProxyReady == readyProxies &&
				mc.Status.PrefillReady == readyPrefills &&
				mc.Status.DecodeReady == readyDecodes)) &&
		mc.Status.ObservedGeneration == mc.Generation {
		return nil
	}

	mc.Status.MasterReady = readyMasters
	mc.Status.WorkerReady = readyWorkers
	if vllmConfigured {
		mc.Status.ProxyReady = readyProxies
		mc.Status.PrefillReady = readyPrefills
		mc.Status.DecodeReady = readyDecodes
	}
	mc.Status.ObservedGeneration = mc.Generation
	mc.Status.Phase = desiredPhase
	statusMsg := fmt.Sprintf("%d/%d masters, %d/%d workers ready",
		readyMasters, mc.Spec.Master.Replicas,
		readyWorkers, mc.Spec.Workers.Replicas)
	if vllmConfigured {
		statusMsg = fmt.Sprintf("%s, %d/%d proxies, %d/%d prefills, %d/%d decodes ready",
			statusMsg,
			readyProxies, mc.Spec.VLLM.Proxy.Replicas,
			readyPrefills, mc.Spec.VLLM.Prefill.Replicas,
			readyDecodes, mc.Spec.VLLM.Decode.Replicas)
	}
	setCondition(&mc.Status, "Ready", "True",
		"PodsReady",
		statusMsg)

	return r.Status().Patch(ctx, mc, patchBase)
}

func (r *MooncakeClusterReconciler) reconcileVLLM(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) (ctrl.Result, error) {
	// 1. Proxy Service
	if err := r.reconcileProxyService(ctx, mc); err != nil {
		return r.setFailed(ctx, mc, "ProxyServiceError", err)
	}

	// 2. Prefill Service
	if err := r.reconcilePrefillService(ctx, mc); err != nil {
		return r.setFailed(ctx, mc, "PrefillServiceError", err)
	}

	// 3. Decode Service
	if err := r.reconcileDecodeService(ctx, mc); err != nil {
		return r.setFailed(ctx, mc, "DecodeServiceError", err)
	}

	// 4. Proxy Deployment
	if err := r.reconcileProxyDeployment(ctx, mc); err != nil {
		return r.setFailed(ctx, mc, "ProxyDeploymentError", err)
	}

	// 5. Prefill Deployment
	if err := r.reconcilePrefillDeployment(ctx, mc); err != nil {
		return r.setFailed(ctx, mc, "PrefillDeploymentError", err)
	}

	// 6. Decode Deployment (with scale-down migration orchestration)
	if err := r.reconcileDecodeDeployment(ctx, mc); err != nil {
		return r.setFailed(ctx, mc, "DecodeDeploymentError", err)
	}

	return ctrl.Result{}, nil
}

func (r *MooncakeClusterReconciler) reconcileProxyService(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildProxyService(mc)
	return r.reconcileService(ctx, mc, desired)
}

func (r *MooncakeClusterReconciler) reconcilePrefillService(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildPrefillService(mc)
	return r.reconcileService(ctx, mc, desired)
}

func (r *MooncakeClusterReconciler) reconcileDecodeService(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildDecodeService(mc)
	return r.reconcileService(ctx, mc, desired)
}

func (r *MooncakeClusterReconciler) reconcileProxyDeployment(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildProxyDeployment(mc)

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
	return r.patchDeploymentSpec(ctx, &existing, desired)
}

func (r *MooncakeClusterReconciler) reconcilePrefillDeployment(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildPrefillDeployment(mc)

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
	return r.patchDeploymentSpec(ctx, &existing, desired)
}

func (r *MooncakeClusterReconciler) reconcileDecodeDeployment(ctx context.Context, mc *mooncakev1alpha1.MooncakeCluster) error {
	desired := r.buildDecodeDeployment(mc)

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

	desiredReplicas := mc.Spec.VLLM.Decode.Replicas
	currentReplicas := int32(0)
	if existing.Spec.Replicas != nil {
		currentReplicas = *existing.Spec.Replicas
	}

	// Scale-down detected: orchestrate KVCache migration before reducing replicas
	if desiredReplicas < currentReplicas {
		if err := r.orchestrateDecodeScaleDown(ctx, mc, desired.Name, int(currentReplicas-desiredReplicas)); err != nil {
			r.Recorder.Event(mc, corev1.EventTypeWarning, "MigrationError", err.Error())
			return err
		}
	}

	if deploymentSpecsEqual(&existing.Spec, &desired.Spec) {
		return nil
	}
	return r.patchDeploymentSpec(ctx, &existing, desired)
}

// orchestrateDecodeScaleDown migrates active requests off terminating decode pods
// before the Deployment replicas are reduced.
func (r *MooncakeClusterReconciler) orchestrateDecodeScaleDown(
	ctx context.Context,
	mc *mooncakev1alpha1.MooncakeCluster,
	deployName string,
	terminateCount int,
) error {
	logger := log.FromContext(ctx)
	logger.Info("decode scale-down detected, orchestrating migration",
		"terminateCount", terminateCount)

	// List all decode pods, sorted by name (highest index = most recent = terminate first)
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(mc.Namespace),
		client.MatchingLabels{"app": "mooncake-decode", "cluster": mc.Name},
	); err != nil {
		return fmt.Errorf("listing decode pods: %w", err)
	}

	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].Name > pods.Items[j].Name
	})

	if len(pods.Items) < terminateCount {
		return fmt.Errorf("not enough decode pods: need %d, have %d", terminateCount, len(pods.Items))
	}

	terminatingPods := pods.Items[:terminateCount]
	alivePods := pods.Items[terminateCount:]

	logger.Info("migrating decode pods",
		"terminating", len(terminatingPods), "alive", len(alivePods))

	for _, pod := range terminatingPods {
		logger.Info("migrating pod", "pod", pod.Name, "ip", pod.Status.PodIP)

		if !isPodReady(&pod) {
			logger.Info("pod not ready, skipping migration", "pod", pod.Name)
			continue
		}

		if err := r.migrateDecodePod(ctx, mc, &pod, alivePods); err != nil {
			logger.Error(err, "migration failed for pod", "pod", pod.Name)
			r.Recorder.Event(mc, corev1.EventTypeWarning, "PodMigrationFailed",
				fmt.Sprintf("migration failed for %s: %v", pod.Name, err))
			continue
		}

		// Mark migrated pod with annotation
		patchBase := client.MergeFrom(pod.DeepCopy())
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations["mooncake.io/migrated"] = "true"
		if err := r.Patch(ctx, &pod, patchBase); err != nil {
			logger.Error(err, "failed to annotate migrated pod", "pod", pod.Name)
		}

		r.Recorder.Event(mc, corev1.EventTypeNormal, "PodMigrated",
			fmt.Sprintf("pod %s migrated successfully", pod.Name))
	}

	return nil
}

// migrateDecodePod migrates active requests from a source decode pod to available
// target decode pods by calling the connector's migration HTTP API directly on
// each pod's port 18900.
func (r *MooncakeClusterReconciler) migrateDecodePod(
	ctx context.Context,
	mc *mooncakev1alpha1.MooncakeCluster,
	source *corev1.Pod,
	alivePods []corev1.Pod,
) error {
	sourceIP := source.Status.PodIP
	if sourceIP == "" {
		return fmt.Errorf("source pod %s has no IP", source.Name)
	}
	if len(alivePods) == 0 {
		return fmt.Errorf("no alive target decode pods for migration")
	}

	migrationPort := 18900
	timeout := mc.Spec.VLLM.Migration.TimeoutSeconds
	if timeout <= 0 {
		timeout = 300
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Step 1: Query active requests on the source pod
	activeReqURL := fmt.Sprintf("http://%s:%d/api/v1/active_requests", sourceIP, migrationPort)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, activeReqURL, nil)
	if err != nil {
		return fmt.Errorf("creating active_requests request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("querying active_requests on %s: %w", sourceIP, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading active_requests response: %w", err)
	}

	var activeResp struct {
		RequestIDs []string `json:"request_ids"`
	}
	if err := json.Unmarshal(body, &activeResp); err != nil {
		return fmt.Errorf("parsing active_requests response: %w", err)
	}

	if len(activeResp.RequestIDs) == 0 {
		return nil // nothing to migrate
	}

	logger := log.FromContext(ctx)
	logger.Info("migrating active requests", "count", len(activeResp.RequestIDs), "source", source.Name)

	// Step 2: For each active request, prepare migration on a target and start migration
	for _, reqID := range activeResp.RequestIDs {
		if time.Now().After(deadline) {
			return fmt.Errorf("migration deadline exceeded for request %s", reqID)
		}

		// Select target (round-robin based on request ID hash)
		targetIdx := int(reqID[0]) % len(alivePods)
		target := &alivePods[targetIdx]
		targetIP := target.Status.PodIP
		if targetIP == "" {
			return fmt.Errorf("target pod %s has no IP", target.Name)
		}

		// Step 2a: Call prepare_migration on target pod (port 18900)
		preparePayload := map[string]interface{}{
			"request_id":   reqID,
			"num_blocks":   128,
			"extra_blocks": 16,
		}
		prepareBody, _ := json.Marshal(preparePayload)

		prepareURL := fmt.Sprintf("http://%s:%d/api/v1/prepare_migration", targetIP, migrationPort)
		prepareResp, err := httpClient.Post(prepareURL, "application/json", bytes.NewReader(prepareBody))
		if err != nil {
			return fmt.Errorf("prepare_migration for request %s on target %s: %w", reqID, targetIP, err)
		}
		prepareRespBody, _ := io.ReadAll(prepareResp.Body)
		prepareResp.Body.Close()

		var targetInfo struct {
			RequestID        string `json:"request_id"`
			TargetBlockIDs   []int  `json:"target_block_ids"`
			KVCachesBaseAddr []int  `json:"kv_caches_base_addr"`
			Error            string `json:"error,omitempty"`
		}
		if err := json.Unmarshal(prepareRespBody, &targetInfo); err != nil {
			return fmt.Errorf("parsing prepare_migration response: %w", err)
		}
		if targetInfo.Error != "" {
			return fmt.Errorf("prepare_migration error: %s", targetInfo.Error)
		}

		// Step 2b: Build block_id_map (source block_id -> target block_id)
		blockIDMap := make(map[string]int)
		for i, bid := range targetInfo.TargetBlockIDs {
			blockIDMap[strconv.Itoa(i)] = bid
		}

		// Step 2c: Call start_migration on source pod (port 18900)
		startPayload := map[string]interface{}{
			"request_id":             reqID,
			"target_host":            targetIP,
			"target_port":            8200,
			"target_base_addr":       targetInfo.KVCachesBaseAddr,
			"block_id_map":           blockIDMap,
			"extra_target_block_ids": targetInfo.TargetBlockIDs[len(targetInfo.TargetBlockIDs)-16:],
		}
		startBody, _ := json.Marshal(startPayload)

		startURL := fmt.Sprintf("http://%s:%d/api/v1/start_migration", sourceIP, migrationPort)
		startResp, err := httpClient.Post(startURL, "application/json", bytes.NewReader(startBody))
		if err != nil {
			return fmt.Errorf("start_migration for request %s on source %s: %w", reqID, sourceIP, err)
		}
		startRespBody, _ := io.ReadAll(startResp.Body)
		startResp.Body.Close()

		var startResult struct {
			Status string `json:"status"`
			Phase  string `json:"phase"`
			Error  string `json:"error,omitempty"`
		}
		if err := json.Unmarshal(startRespBody, &startResult); err != nil {
			return fmt.Errorf("parsing start_migration response: %w", err)
		}
		if startResult.Error != "" {
			return fmt.Errorf("start_migration error: %s", startResult.Error)
		}

		logger.Info("migration started for request",
			"request_id", reqID, "target", target.Name, "phase", startResult.Phase)
	}

	// Step 3: Poll migration status until all requests complete or timeout
	for _, reqID := range activeResp.RequestIDs {
		for {
			if time.Now().After(deadline) {
				return fmt.Errorf("migration status polling deadline exceeded for request %s", reqID)
			}

			statusURL := fmt.Sprintf("http://%s:%d/api/v1/migration/status/%s", sourceIP, migrationPort, reqID)
			statusResp, err := httpClient.Get(statusURL)
			if err != nil {
				// Pod may be shutting down — assume success
				break
			}

			statusBody, _ := io.ReadAll(statusResp.Body)
			statusResp.Body.Close()

			var statusResult struct {
				Phase string `json:"phase"`
			}
			if err := json.Unmarshal(statusBody, &statusResult); err != nil {
				break
			}

			if statusResult.Phase == "COMPLETED" || statusResult.Phase == "SWITCH_OVER" {
				logger.Info("migration completed for request", "request_id", reqID)
				break
			}

			// Wait before next poll
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}

	return nil
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

	// Eviction config — use safe defaults if values are at Go zero value (unset)
	evictionRatio := mc.Spec.Eviction.Ratio
	if evictionRatio <= 0 {
		evictionRatio = 0.1
	}
	evictionHighWatermark := mc.Spec.Eviction.HighWatermarkRatio
	if evictionHighWatermark <= 0 {
		evictionHighWatermark = 0.8
	}
	config["eviction_ratio"] = evictionRatio
	config["eviction_high_watermark_ratio"] = evictionHighWatermark
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
