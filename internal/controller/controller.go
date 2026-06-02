package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

const xasFinalizer = "xas.io/finalizer"

type Controller struct {
	kubeclientset        kubernetes.Interface
	xasclientset         clientset.Interface
	policiesLister       listers.ScalingPolicyLister
	metricProviderLister listers.MetricProviderClassLister
	recommenderLister    listers.RecommenderClassLister
	policiesSynced       cache.InformerSynced
	workqueue            workqueue.RateLimitingInterface

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASControlPlaneClient

	clusterName string
}

func NewController(
	kubeclientset kubernetes.Interface,
	xasclientset clientset.Interface,
	policyInformer informers.ScalingPolicyInformer,
	metricProviderInformer informers.MetricProviderClassInformer,
	recommenderInformer informers.RecommenderClassInformer,
	serverAddress string,
	clusterName string) *Controller {

	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASControlPlaneClient(conn)

	c := &Controller{
		kubeclientset:        kubeclientset,
		xasclientset:         xasclientset,
		policiesLister:       policyInformer.Lister(),
		metricProviderLister: metricProviderInformer.Lister(),
		recommenderLister:    recommenderInformer.Lister(),
		policiesSynced:       policyInformer.Informer().HasSynced,
		workqueue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "ScalingPolicies"),
		grpcConn:             conn,
		grpcClient:           client,
		clusterName:          clusterName,
	}

	slog.Info("Setting up event handlers")
	policyInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueuePolicy,
		UpdateFunc: func(old, new interface{}) {
			c.enqueuePolicy(new)
		},
		DeleteFunc: c.enqueuePolicy,
	})

	return c
}

func (c *Controller) Run(workers int, ctx context.Context) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	defer c.grpcConn.Close()

	slog.Info("Starting XAS controller")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.policiesSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	slog.Info("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, ctx.Done())
	}

	// Start a poller loop to re-enqueue items periodically
	go wait.Until(c.poller, 5*time.Second, ctx.Done())

	slog.Info("Started workers")
	<-ctx.Done()
	slog.Info("Shutting down workers")
	return nil
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.syncHandler(key); err != nil {
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.workqueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
	}
	return true
}

func (c *Controller) syncHandler(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	policy, err := c.policiesLister.ScalingPolicies(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.reconcileDelete(namespace, name)
		}
		return err
	}

	return c.reconcilePolicy(policy)
}

func (c *Controller) validateReferences(p *xasv1.ScalingPolicy) error {
	for _, m := range p.Spec.Metrics {
		if _, err := c.metricProviderLister.Get(m.Provider); err != nil {
			return fmt.Errorf("metric provider class '%s' not found", m.Provider)
		}
	}
	for _, r := range p.Spec.Activation {
		if _, err := c.recommenderLister.Get(r.Recommender); err != nil {
			return fmt.Errorf("recommender class '%s' not found in activation", r.Recommender)
		}
	}
	for _, r := range p.Spec.Scaling {
		if _, err := c.recommenderLister.Get(r.Recommender); err != nil {
			return fmt.Errorf("recommender class '%s' not found in scaling", r.Recommender)
		}
	}
	return nil
}

func (c *Controller) reconcileDelete(namespace, name string) error {
	slog.Info("Policy deleted in K8s, syncing deletion to Control Plane", "namespace", namespace, "name", name)
	req := &pb.DeletePolicyRequest{
		Id: &pb.PolicyId{
			ClusterName: c.clusterName,
			Namespace:   namespace,
			Name:        name,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.grpcClient.DeletePolicy(ctx, req)
	return err
}

func (c *Controller) reconcilePolicy(policy *xasv1.ScalingPolicy) error {
	// Check for deletion
	if !policy.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is being deleted
		if containsString(policy.ObjectMeta.Finalizers, xasFinalizer) {
			// our finalizer is present, so lets handle any external dependency
			if err := c.reconcileDelete(policy.Namespace, policy.Name); err != nil {
				// if fail to delete the external dependency here, return with error
				// so that it can be retried
				return err
			}

			// remove our finalizer from the list and update it.
			policy.ObjectMeta.Finalizers = removeString(policy.ObjectMeta.Finalizers, xasFinalizer)
			if _, err := c.xasclientset.XasV1().ScalingPolicies(policy.Namespace).Update(context.TODO(), policy, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
		return nil
	}

	// Add finalizer if missing
	if !containsString(policy.ObjectMeta.Finalizers, xasFinalizer) {
		policy.ObjectMeta.Finalizers = append(policy.ObjectMeta.Finalizers, xasFinalizer)
		if _, err := c.xasclientset.XasV1().ScalingPolicies(policy.Namespace).Update(context.TODO(), policy, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	// 0. Validate References
	if err := c.validateReferences(policy); err != nil {
		// TODO: Update status with validation error condition
		slog.Error("Validation failed for policy", "namespace", policy.Namespace, "name", policy.Name, "error", err)
		return nil // Do not retry until policy is updated
	}

	// 1. Get Target Deployment
	deploymentName := policy.Spec.ScaleTargetRef.Name
	var deployment *appsv1.Deployment
	var err error
	deployment, err = c.kubeclientset.AppsV1().Deployments(policy.Namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// 2. Push Policy to Control Plane
	if err := c.pushPolicy(policy, deployment); err != nil {
		slog.Error("Failed to push policy to control plane", "error", err)
		return nil
	}

	// 3. Sync Workload State (Topology) to Control Plane
	if err := c.syncWorkload(policy, deployment); err != nil {
		slog.Error("Failed to sync workload", "error", err)
		return nil
	}

	// 4. Poll Control Plane
	resp, err := c.getRecommendation(policy.Namespace, policy.Name)
	if err != nil {
		slog.Error("Failed to get recommendation", "deployment", deploymentName, "error", err)
		return nil
	}

	if resp.Recommendation == nil {
		slog.Debug("No recommendation available for policy", "policy", policy.Name)
		return nil
	}

	rec := resp.Recommendation
	slog.Debug("Got recommendation", "policy", policy.Name, "target", rec.TargetReplicas)
	slog.Info("Recommendation", "deployment", deploymentName, "targetReplicas", rec.TargetReplicas, "currentDesired", *deployment.Spec.Replicas)

	// 5. Actuate
	desired := rec.TargetReplicas

	var workloadRes *pb.ResourceRecommendation
	var podRes []*pb.PodResourceRecommendation
	for _, exp := range rec.Explanation {
		if exp.WorkloadResources != nil {
			workloadRes = exp.WorkloadResources
		}
		if len(exp.PodResources) > 0 {
			podRes = append(podRes, exp.PodResources...)
		}
	}

	needsUpdate := false
	deploymentCopy := deployment.DeepCopy()

	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != desired {
		slog.Debug("Actuating scale", "deployment", deploymentName, "to", desired)
		slog.Info("SCALING", "deployment", deploymentName, "from", *deployment.Spec.Replicas, "to", desired)
		deploymentCopy.Spec.Replicas = &desired
		needsUpdate = true
	}

	if needsUpdate {
		_, err := c.kubeclientset.AppsV1().Deployments(policy.Namespace).Update(context.TODO(), deploymentCopy, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	// Helper to patch pod resize
	patchPodResize := func(pod *corev1.Pod, containerName string, requests, limits map[string]string) {
		var targetContainer *corev1.Container
		for i, c := range pod.Spec.Containers {
			if c.Name == containerName {
				targetContainer = &pod.Spec.Containers[i]
				break
			}
		}
		if targetContainer == nil {
			return
		}

		needsPatch := false
		for k, v := range requests {
			q, err := resource.ParseQuantity(v)
			if err != nil {
				continue
			}
			current, exists := targetContainer.Resources.Requests[corev1.ResourceName(k)]
			if !exists || current.Cmp(q) != 0 {
				needsPatch = true
				break
			}
		}

		if !needsPatch {
			for k, v := range limits {
				q, err := resource.ParseQuantity(v)
				if err != nil {
					continue
				}
				current, exists := targetContainer.Resources.Limits[corev1.ResourceName(k)]
				if !exists || current.Cmp(q) != 0 {
					needsPatch = true
					break
				}
			}
		}

		if !needsPatch {
			return
		}

		reqs := make(map[string]string)
		for k, v := range requests {
			reqs[k] = v
		}
		lims := make(map[string]string)
		for k, v := range limits {
			lims[k] = v
		}

		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"containers": []map[string]interface{}{
					{
						"name": containerName,
						"resources": map[string]interface{}{
							"requests": reqs,
							"limits":   lims,
						},
					},
				},
			},
		}
		patchBytes, _ := json.Marshal(patch)
		_, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).Patch(context.TODO(), pod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "resize")
		if err != nil {
			slog.Error("Failed to patch pod resize", "pod", pod.Name, "error", err)
		} else {
			slog.Info("Patched pod resize successfully", "pod", pod.Name)
		}
	}

	// Actuate Workload Resources via /resize subresource on all matching pods
	if workloadRes != nil {
		selector := labels.Set(deployment.Spec.Selector.MatchLabels).String()
		pods, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
		if err == nil {
			for i := range pods.Items {
				pod := &pods.Items[i]
				if len(pod.Spec.Containers) > 0 {
					patchPodResize(pod, pod.Spec.Containers[0].Name, workloadRes.Requests, workloadRes.Limits)
				}
			}
		}
	}

	// Actuate Pod Resources via /resize subresource
	for _, pr := range podRes {
		pod, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).Get(context.TODO(), pr.PodName, metav1.GetOptions{})
		if err == nil && len(pod.Spec.Containers) > 0 {
			patchPodResize(pod, pod.Spec.Containers[0].Name, pr.Requests, pr.Limits)
		}
	}

	// Actuation Policy & Fallback
	if policy.Spec.ActuationPolicy != nil && policy.Spec.ActuationPolicy.InPlaceFallback == "EvictOnFailure" {
		selector := labels.Set(deployment.Spec.Selector.MatchLabels).String()
		pods, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
		if err == nil {
			for _, pod := range pods.Items {
				if string(pod.Status.Resize) == string(corev1.PodResizeStatusInfeasible) {
					slog.Info("Evicting pod due to infeasible resize", "pod", pod.Name)
					_ = c.kubeclientset.CoreV1().Pods(policy.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
				}
			}
		}
	}

	// 5. Update Status
	policyCopy := policy.DeepCopy()
	policyCopy.Status.CurrentReplicas = *deployment.Spec.Replicas
	policyCopy.Status.DesiredReplicas = desired
	policyCopy.Status.LastUpdated = time.Now().Format(time.RFC3339)

	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		slog.Error("Error parsing selector", "error", err)
	} else {
		policyCopy.Status.Selector = selector.String()
	}

	policyCopy.Status.Decisions = make([]xasv1.DecisionStatus, len(rec.Explanation))
	for i, d := range rec.Explanation {
		var wr *xasv1.ResourceRecommendation
		if d.WorkloadResources != nil {
			wr = &xasv1.ResourceRecommendation{
				Requests: d.WorkloadResources.Requests,
				Limits:   d.WorkloadResources.Limits,
			}
		}

		var prs []xasv1.PodResourceRecommendation
		for _, pr := range d.PodResources {
			prs = append(prs, xasv1.PodResourceRecommendation{
				PodName:  pr.PodName,
				Requests: pr.Requests,
				Limits:   pr.Limits,
			})
		}

		policyCopy.Status.Decisions[i] = xasv1.DecisionStatus{
			RecommenderName:   d.Name,
			Type:              d.Type,
			Phase:             d.Phase,
			Mode:              d.Mode,
			Active:            d.IsActive,
			Replicas:          d.DesiredReplicas,
			Reason:            d.Message,
			WorkloadResources: wr,
			PodResources:      prs,
		}
	}

	policyCopy.Status.MetricStatuses = make([]xasv1.MetricStatus, len(resp.MetricStatuses))
	for i, ms := range resp.MetricStatuses {
		policyCopy.Status.MetricStatuses[i] = xasv1.MetricStatus{
			Name:        ms.Name,
			LastUpdated: time.Unix(ms.Timestamp, 0).Format(time.RFC3339),
			Value:       fmt.Sprintf("%g", ms.Value),
			Error:       ms.Error,
		}
	}

	_, err = c.xasclientset.XasV1().ScalingPolicies(policy.Namespace).UpdateStatus(context.TODO(), policyCopy, metav1.UpdateOptions{})
	return err
}
func (c *Controller) getRecommendation(namespace, policyName string) (*pb.GetRecommendationResponse, error) {
	req := &pb.GetRecommendationRequest{
		Id: &pb.PolicyId{ClusterName: c.clusterName, Namespace: namespace, Name: policyName},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.grpcClient.GetRecommendation(ctx, req)
}

func (c *Controller) pushPolicy(p *xasv1.ScalingPolicy, deployment *appsv1.Deployment) error {
	gv := p.Spec.ScaleTargetRef.APIVersion
	var group, version string
	if strings.Contains(gv, "/") {
		parts := strings.Split(gv, "/")
		group = parts[0]
		version = parts[1]
	} else {
		group = ""
		version = gv
	}

	var metrics []*pb.MetricDefinition
	for _, m := range p.Spec.Metrics {
		pm := &pb.MetricDefinition{
			Name:     m.Name,
			Provider: m.Provider,
			Params:   m.Params,
			Filter:   m.Filter,
			Scope:    m.Scope,
		}

		// Sync Intents
		if m.Gauge != nil {
			pm.Gauge = &pb.Gauge{
				Aggregation: m.Gauge.Aggregation,
			}
		}
		if m.Rate != nil {
			pm.Rate = &pb.Rate{
				Window:      m.Rate.Window.Duration.String(),
				Aggregation: m.Rate.Aggregation,
			}
		}
		if m.Distribution != nil {
			pm.Distribution = &pb.Distribution{
				Percentile:  m.Distribution.Percentile,
				Aggregation: m.Distribution.Aggregation,
			}
		}
		if m.DecayingDistribution != nil {
			pm.DecayingDistribution = &pb.DecayingDistribution{
				HalfLife:   m.DecayingDistribution.HalfLife.Duration.String(),
				BucketSize: m.DecayingDistribution.BucketSize,
				Percentile: m.DecayingDistribution.Percentile,
			}
			if m.DecayingDistribution.Rate != nil {
				pm.DecayingDistribution.Rate = m.DecayingDistribution.Rate.Duration.String()
			}
		}

		metrics = append(metrics, pm)
	}

	var activation []*pb.RecommenderDefinition
	for _, r := range p.Spec.Activation {
		recType := ""
		if class, err := c.recommenderLister.Get(r.Recommender); err == nil {
			recType = class.Spec.Type
		}
		activation = append(activation, &pb.RecommenderDefinition{
			Recommender: r.Recommender,
			Name:        r.Name,
			Mode:        r.Mode,
			Params:      r.Params,
			Type:        recType,
		})
	}

	var scaling []*pb.RecommenderDefinition
	for _, r := range p.Spec.Scaling {
		recType := ""
		if class, err := c.recommenderLister.Get(r.Recommender); err == nil {
			recType = class.Spec.Type
		}
		scaling = append(scaling, &pb.RecommenderDefinition{
			Recommender: r.Recommender,
			Name:        r.Name,
			Mode:        r.Mode,
			Params:      r.Params,
			Type:        recType,
		})
	}

	pol := &pb.Policy{
		Id: &pb.PolicyId{
			ClusterName: c.clusterName,
			Namespace:   p.Namespace,
			Name:        p.Name,
		},
		Workload: &pb.WorkloadRef{
			Group:     group,
			Version:   version,
			Kind:      p.Spec.ScaleTargetRef.Kind,
			Name:      p.Spec.ScaleTargetRef.Name,
			Namespace: p.Namespace,
		},
		MinReplicas: 1,
		MaxReplicas: p.Spec.MaxReplicas,
		Metrics:     metrics,
		Activation:  activation,
		Scaling:     scaling,
		Selector:    labels.Set(deployment.Spec.Selector.MatchLabels).String(),
	}
	if p.Spec.MinReplicas != nil {
		pol.MinReplicas = *p.Spec.MinReplicas
	}

	req := &pb.UpdatePolicyRequest{
		Policy: pol,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.grpcClient.UpdatePolicy(ctx, req)
	return err
}

func (c *Controller) enqueuePolicy(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) poller() {
	allPolicies, err := c.policiesLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, p := range allPolicies {
		key, err := cache.MetaNamespaceKeyFunc(p)
		if err == nil {
			c.workqueue.Add(key)
		}
	}
}

func (c *Controller) syncWorkload(policy *xasv1.ScalingPolicy, deployment *appsv1.Deployment) error {
	// 1. List Pods
	selector := labels.Set(deployment.Spec.Selector.MatchLabels).String()
	pods, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}

	// 2. Build PodStates
	var replicas []*pb.PodState
	for _, pod := range pods.Items {
		isReady := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				isReady = true
				break
			}
		}
		replicas = append(replicas, &pb.PodState{
			Name:     pod.Name,
			NodeName: pod.Spec.NodeName,
			IsReady:  isReady,
		})
	}

	// 3. Send
	req := &pb.UpdateWorkloadRequest{
		Id: &pb.PolicyId{ClusterName: c.clusterName, Namespace: policy.Namespace, Name: policy.Name},
		Workload: &pb.Workload{
			Pods: replicas,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = c.grpcClient.UpdateWorkload(ctx, req)
	return err
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	var result []string
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return result
}
