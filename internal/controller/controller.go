package controller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/controller/watchclient"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/podutil"
	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

const (
	xasFinalizer    = "xas.io/finalizer"
	recChanCapacity = 100
)

// policyID identifies a ScalingPolicy in the workqueue.
type policyID struct {
	namespace string
	name      string
}

func policyIDOf(p *xasv1.ScalingPolicy) policyID {
	return policyID{namespace: p.Namespace, name: p.Name}
}

// protoPolicyID converts a policyID to the gRPC PolicyId, scoped to this controller's cluster.
func (c *Controller) protoPolicyID(id policyID) *pb.PolicyId {
	return pb.PolicyId_builder{
		ClusterName: new(c.clusterName),
		Namespace:   new(id.namespace),
		Name:        new(id.name),
	}.Build()
}

type Controller struct {
	kubeclientset        kubernetes.Interface
	xasclientset         clientset.Interface
	scaleClient          scale.ScalesGetter
	mapper               meta.RESTMapper
	policiesLister       listers.ScalingPolicyLister
	metricProviderLister listers.MetricProviderClassLister
	recommenderLister    listers.RecommenderClassLister
	policiesSynced       cache.InformerSynced
	workqueue            workqueue.TypedRateLimitingInterface[policyID]
	state                *State

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASServerClient

	clusterName string
}

func NewController(
	kubeclientset kubernetes.Interface,
	xasclientset clientset.Interface,
	scaleClient scale.ScalesGetter,
	mapper meta.RESTMapper,
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
	client := pb.NewXASServerClient(conn)

	c := &Controller{
		kubeclientset:        kubeclientset,
		xasclientset:         xasclientset,
		scaleClient:          scaleClient,
		mapper:               mapper,
		policiesLister:       policyInformer.Lister(),
		metricProviderLister: metricProviderInformer.Lister(),
		recommenderLister:    recommenderInformer.Lister(),
		policiesSynced:       policyInformer.Informer().HasSynced,
		workqueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[policyID](),
			workqueue.TypedRateLimitingQueueConfig[policyID]{Name: "ScalingPolicies"},
		),
		state:       NewState(),
		grpcConn:    conn,
		grpcClient:  client,
		clusterName: clusterName,
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

	slog.Info("Starting xAS controller")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.policiesSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	slog.Info("Starting workers")
	for range workers {
		go wait.Until(c.runWorker, time.Second, ctx.Done())
	}

	// Start the recommendation stream and its consumer.
	recChan := make(chan *pb.ArbitratedRecommendationsEvent, recChanCapacity)
	recClient := watchclient.NewRecommendationsClient(c.grpcConn, c.clusterName)
	go recClient.Run(ctx, recChan)
	go c.handleRecommendations(ctx, recChan)

	// Start a poller loop to re-enqueue items periodically
	go wait.Until(c.poller, 5*time.Second, ctx.Done())

	slog.Info("Started workers")
	<-ctx.Done()
	slog.Info("Shutting down workers")
	return nil
}

// handleRecommendations consumes streamed recommendations and, for each one,
// actuates it on the target workload and updates the policy status.
func (c *Controller) handleRecommendations(ctx context.Context, ch <-chan *pb.ArbitratedRecommendationsEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-ch:
			if err := c.handleRecommendation(resp); err != nil {
				slog.Error("Failed to handle streamed recommendation",
					"namespace", resp.GetId().GetNamespace(), "name", resp.GetId().GetName(), "error", err)
			}
		}
	}
}

// handleRecommendation applies a single streamed recommendation: it resolves
// the policy and its target deployment, then runs actuate and updateStatus.
func (c *Controller) handleRecommendation(resp *pb.ArbitratedRecommendationsEvent) error {
	id := policyID{namespace: resp.GetId().GetNamespace(), name: resp.GetId().GetName()}

	policy, err := c.policiesLister.ScalingPolicies(id.namespace).Get(id.name)
	if err != nil {
		return fmt.Errorf("getting policy: %w", err)
	}
	if !policy.ObjectMeta.DeletionTimestamp.IsZero() {
		return nil // Policy is being deleted; nothing to actuate.
	}

	// The pod selector is cached by reconcilePolicy, which is the only writer.
	selector, ok := c.state.getSelector(id)
	if !ok {
		return fmt.Errorf("no cached selector for policy; it has not been reconciled yet")
	}

	// The /scale subresource carries the current replica count and works for
	// any scalable resource, not just Deployments.
	targetResource, err := c.scaleTargetResource(policy.Spec.ScaleTargetRef)
	if err != nil {
		return fmt.Errorf("resolving scale target: %w", err)
	}
	scale, err := c.scaleClient.Scales(policy.Namespace).
		Get(context.TODO(), targetResource, policy.Spec.ScaleTargetRef.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting scale for %s %s: %w", targetResource, policy.Spec.ScaleTargetRef.Name, err)
	}

	// actuate and updateStatus operate on a ArbitratedRecommendation, which
	// carries the same payload as the streamed message minus the policy id.
	recResp := pb.ArbitratedRecommendation_builder{
		Recommendation: resp.GetRecommendation(),
		Explanation:    resp.GetExplanation(),
		MetricStatuses: resp.GetMetricStatuses(),
	}.Build()

	if recResp.HasRecommendation() {
		if err := c.actuate(policy, targetResource, scale, selector, recResp.GetRecommendation()); err != nil {
			return fmt.Errorf("actuating: %w", err)
		}
	}
	return c.updateStatus(policy, scale, selector, recResp)
}

// scaleTargetResource resolves a policy's scaleTargetRef (apiVersion + kind)
// to the GroupResource expected by the scale client, using API discovery.
// An empty apiVersion defaults to apps/v1 for backward compatibility.
func (c *Controller) scaleTargetResource(ref xasv1.CrossVersionObjectReference) (schema.GroupResource, error) {
	apiVersion := ref.APIVersion
	if apiVersion == "" {
		apiVersion = "apps/v1"
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupResource{}, fmt.Errorf("invalid apiVersion %q: %w", apiVersion, err)
	}
	mapping, err := c.mapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: ref.Kind}, gv.Version)
	if err != nil {
		return schema.GroupResource{}, fmt.Errorf("mapping %s/%s: %w", apiVersion, ref.Kind, err)
	}
	return mapping.Resource.GroupResource(), nil
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	id, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}
	defer c.workqueue.Done(id)

	if err := c.syncHandler(id); err != nil {
		c.workqueue.AddRateLimited(id)
		utilruntime.HandleError(fmt.Errorf("error syncing '%s/%s': %w, requeuing", id.namespace, id.name, err))
		return true
	}
	c.workqueue.Forget(id)
	return true
}

func (c *Controller) syncHandler(id policyID) error {
	policy, err := c.policiesLister.ScalingPolicies(id.namespace).Get(id.name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.reconcileDelete(id)
		}
		return err
	}

	return c.reconcilePolicy(policy)
}

// validateReferences returns an error if a policy refers to unknown
// recommenders or metric providers.
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

func (c *Controller) reconcileDelete(id policyID) error {
	slog.Info("Policy deleted in K8s, syncing deletion to Server", "namespace", id.namespace, "name", id.name)
	c.state.delete(id)
	req := pb.DeletePolicyRequest_builder{
		Id: c.protoPolicyID(id),
	}.Build()
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
			if err := c.reconcileDelete(policyIDOf(policy)); err != nil {
				// if fail to delete the external dependency here, return with error
				// so that it can be retried
				return err
			}

			// remove our finalizer from the list and update it.
			policy.ObjectMeta.Finalizers = removeString(policy.ObjectMeta.Finalizers, xasFinalizer)
			if _, err := c.xasclientset.XASV1().ScalingPolicies(policy.Namespace).Update(context.TODO(), policy, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
		return nil
	}

	// Add finalizer if missing
	if !containsString(policy.ObjectMeta.Finalizers, xasFinalizer) {
		policy.ObjectMeta.Finalizers = append(policy.ObjectMeta.Finalizers, xasFinalizer)
		if _, err := c.xasclientset.XASV1().ScalingPolicies(policy.Namespace).Update(context.TODO(), policy, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	// 0. Warn about unknown recommenders or metric providers.
	if err := c.validateReferences(policy); err != nil {
		// TODO: Update status with validation error condition
		slog.Error("Validation failed for policy", "namespace", policy.Namespace, "name", policy.Name, "error", err)
		return nil // Do not retry until policy is updated
	}

	// 1. Get the target workload's /scale subresource and cache its selector.
	targetResource, err := c.scaleTargetResource(policy.Spec.ScaleTargetRef)
	if err != nil {
		return fmt.Errorf("resolving scale target: %w", err)
	}
	scale, err := c.scaleClient.Scales(policy.Namespace).
		Get(context.TODO(), targetResource, policy.Spec.ScaleTargetRef.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting scale for %s %s: %w", targetResource, policy.Spec.ScaleTargetRef.Name, err)
	}
	selector := scale.Status.Selector
	c.state.setSelector(policyIDOf(policy), selector)

	// 2. Push Policy to Server
	if err := c.pushPolicy(policy, selector); err != nil {
		slog.Error("Failed to push policy to server", "error", err)
		return nil
	}

	// 3. Sync Workload State to Server
	if err := c.syncWorkload(policy, selector); err != nil {
		slog.Error("Failed to sync workload", "error", err)
		return nil
	}
	return nil
}

// actuate applies a recommendation to the target workload and its pods:
// horizontal scaling via the /scale subresource, in-place vertical scaling via
// the pod /resize subresource, and the eviction fallback for infeasible
// resizes when configured by the policy.
func (c *Controller) actuate(policy *xasv1.ScalingPolicy, targetResource schema.GroupResource, scale *autoscalingv1.Scale, selector string, rec *pb.Recommendation) error {
	deploymentName := scale.Name
	currentReplicas := scale.Spec.Replicas
	slog.Info("Recommendation", "deployment", deploymentName, "replicas", proto.ValueOrNil(rec.HasReplicas(), rec.GetReplicas), "currentReplicas", currentReplicas)

	// If there's a horizontal recommendation, and it differs from the workload's current value, actuate.
	if rec.HasReplicas() && currentReplicas != rec.GetReplicas() {
		slog.Info("SCALING", "deployment", deploymentName, "from", currentReplicas, "to", rec.GetReplicas())
		scaleCopy := scale.DeepCopy()
		scaleCopy.Spec.Replicas = rec.GetReplicas()
		if _, err := c.scaleClient.Scales(policy.Namespace).
			Update(context.TODO(), targetResource, scaleCopy, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	// Helper to patch pod resize. An empty containerName targets the pod-level
	// resources (pod.spec.resources).
	patchPodResize := func(pod *corev1.Pod, containerName string, requests, limits map[string]string) {
		patchBytes, skipReason, err := buildResizePatch(pod, containerName, requests, limits)
		if err != nil {
			slog.Error("Failed to build pod resize patch", "pod", pod.Name, "container", containerName, "error", err)
			return
		}
		if skipReason != "" {
			slog.Warn("Skipping pod resize", "pod", pod.Name, "container", containerName, "reason", skipReason)
			return
		}
		if patchBytes == nil {
			return
		}
		_, err = c.kubeclientset.CoreV1().Pods(policy.Namespace).Patch(context.TODO(), pod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "resize")
		if err != nil {
			slog.Error("Failed to patch pod resize", "pod", pod.Name, "container", containerName, "error", err)
		} else {
			slog.Info("Patched pod resize successfully", "pod", pod.Name, "container", containerName, "patch", string(patchBytes))
		}
	}

	// Actuate Workload Resources via /resize subresource on all matching pods
	if len(rec.GetWorkloadResources()) > 0 && selector != "" {
		pods, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
		if err == nil {
			for i := range pods.Items {
				pod := &pods.Items[i]
				for _, res := range rec.GetWorkloadResources() {
					if res == nil {
						continue
					}
					patchPodResize(pod, res.GetContainerName(), res.GetRequests(), res.GetLimits())
				}
			}
		}
	}

	// Actuate Pod Resources via /resize subresource
	for _, pr := range rec.GetPodContainerResources() {
		if pr == nil || !pr.HasContainerResources() {
			continue
		}
		pod, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).Get(context.TODO(), pr.GetPodName(), metav1.GetOptions{})
		if err == nil {
			res := pr.GetContainerResources()
			patchPodResize(pod, res.GetContainerName(), res.GetRequests(), res.GetLimits())
		}
	}

	// Actuation Policy & Fallback
	if policy.Spec.ActuationPolicy != nil && policy.Spec.ActuationPolicy.InPlaceFallback == "EvictOnFailure" && selector != "" {
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

	return nil
}

// updateStatus populates the ScalingPolicy status from the target workload's
// scale and the server's recommendation response, then persists it via
// UpdateStatus.
func (c *Controller) updateStatus(policy *xasv1.ScalingPolicy, scale *autoscalingv1.Scale, selector string, resp *pb.ArbitratedRecommendation) error {
	rec := resp.GetRecommendation()
	policyCopy := policy.DeepCopy()
	policyCopy.Status.CurrentReplicas = scale.Spec.Replicas
	if rec.HasReplicas() {
		policyCopy.Status.DesiredReplicas = rec.GetReplicas()
	} else {
		policyCopy.Status.DesiredReplicas = scale.Spec.Replicas
	}
	policyCopy.Status.LastUpdated = time.Now().Format(time.RFC3339)
	policyCopy.Status.Selector = selector

	policyCopy.Status.Recommendations = make([]xasv1.RecommendationStatus, len(resp.GetExplanation()))
	for i, d := range resp.GetExplanation() {
		var wrr []xasv1.ResourceRecommendation
		for _, wr := range d.GetWorkloadResources() {
			if wr == nil {
				continue
			}
			wrr = append(wrr, xasv1.ResourceRecommendation{
				ContainerName: wr.GetContainerName(),
				Requests:      wr.GetRequests(),
				Limits:        wr.GetLimits(),
			})
		}

		var prr []xasv1.PodResourceRecommendation
		for _, pr := range d.GetPodResources() {
			if pr == nil {
				continue
			}
			rec := xasv1.PodResourceRecommendation{PodName: pr.GetPodName()}
			if cr := pr.GetContainerResources(); cr != nil {
				rec.Resources = &xasv1.ResourceRecommendation{
					ContainerName: cr.GetContainerName(),
					Requests:      cr.GetRequests(),
					Limits:        cr.GetLimits(),
				}
			}
			prr = append(prr, rec)
		}

		policyCopy.Status.Recommendations[i] = xasv1.RecommendationStatus{
			RecommenderName:   d.GetName(),
			Type:              d.GetType(),
			Phase:             d.GetPhase(),
			Mode:              d.GetMode(),
			Active:            d.GetIsActive(),
			Reason:            d.GetMessage(),
			Replicas:          proto.ValueOrNil(d.HasReplicas(), d.GetReplicas),
			WorkloadResources: wrr,
			PodResources:      prr,
		}
	}

	policyCopy.Status.MetricStatuses = make([]xasv1.MetricStatus, len(resp.GetMetricStatuses()))
	for i, ms := range resp.GetMetricStatuses() {
		policyCopy.Status.MetricStatuses[i] = xasv1.MetricStatus{
			Name:        ms.GetName(),
			LastUpdated: ms.GetTimestamp().AsTime().Format(time.RFC3339),
			Value:       fmt.Sprintf("%g", ms.GetValue()),
			Error:       ms.GetError(),
		}
	}

	_, err := c.xasclientset.XASV1().ScalingPolicies(policy.Namespace).UpdateStatus(context.TODO(), policyCopy, metav1.UpdateOptions{})
	return err
}

func (c *Controller) pushPolicy(p *xasv1.ScalingPolicy, selector string) error {
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
		pm := pb.MetricDefinition_builder{
			Name:     proto.String(m.Name),
			Provider: proto.String(m.Provider),
			Params:   m.Params,
			Filter:   m.Filter,
			Scope:    proto.String(m.Scope),
		}.Build()
		if m.Eligibility != nil {
			e := pb.MetricDefinition_Eligibility_builder{
				StartupSamples: proto.Bool(m.Eligibility.StartupSamples),
			}.Build()
			if m.Eligibility.StartupGracePeriod != nil {
				e.SetStartupGracePeriod(durationpb.New(m.Eligibility.StartupGracePeriod.Duration))
			}
			pm.SetEligibility(e)
		}

		// Sync Intents
		if m.Gauge != nil {
			pm.SetGauge(pb.Gauge_builder{
				Aggregation: proto.String(m.Gauge.Aggregation),
			}.Build())
		}
		if m.Rate != nil {
			pm.SetRate(pb.Rate_builder{
				Window:      durationpb.New(m.Rate.Window.Duration),
				Aggregation: proto.String(m.Rate.Aggregation),
			}.Build())
		}
		if m.Distribution != nil {
			pm.SetDistribution(pb.Distribution_builder{
				Percentile:  proto.String(m.Distribution.Percentile),
				Aggregation: proto.String(m.Distribution.Aggregation),
			}.Build())
		}
		if m.DecayingDistribution != nil {
			pm.SetDecayingDistribution(pb.DecayingDistribution_builder{
				HalfLife:   durationpb.New(m.DecayingDistribution.HalfLife.Duration),
				BucketSize: proto.String(m.DecayingDistribution.BucketSize),
				Percentile: proto.String(m.DecayingDistribution.Percentile),
			}.Build())
			if m.DecayingDistribution.Rate != nil {
				pm.GetDecayingDistribution().SetRate(durationpb.New(m.DecayingDistribution.Rate.Duration))
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
		activation = append(activation, pb.RecommenderDefinition_builder{
			Recommender: proto.String(r.Recommender),
			Name:        proto.String(r.Name),
			Mode:        proto.String(r.Mode),
			Params:      r.Params,
			Type:        proto.String(recType),
		}.Build())
	}

	var scaling []*pb.RecommenderDefinition
	for _, r := range p.Spec.Scaling {
		recType := ""
		if class, err := c.recommenderLister.Get(r.Recommender); err == nil {
			recType = class.Spec.Type
		}
		scaling = append(scaling, pb.RecommenderDefinition_builder{
			Recommender: proto.String(r.Recommender),
			Name:        proto.String(r.Name),
			Mode:        proto.String(r.Mode),
			Params:      r.Params,
			Type:        proto.String(recType),
		}.Build())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policyId := c.protoPolicyID(policyIDOf(p))
	getPoliciesReq := pb.GetPolicyRequest_builder{
		Id: policyId,
	}.Build()

	sp, err := c.grpcClient.GetPolicy(ctx, getPoliciesReq)
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("unable to get policy: %w", err)
	}
	pol := pb.Policy_builder{
		Id: policyId,
		Workload: pb.WorkloadRef_builder{
			Group:     proto.String(group),
			Version:   proto.String(version),
			Kind:      proto.String(p.Spec.ScaleTargetRef.Kind),
			Name:      proto.String(p.Spec.ScaleTargetRef.Name),
			Namespace: proto.String(p.Namespace),
		}.Build(),
		MinReplicas: proto.Int32(1),
		MaxReplicas: proto.Int32(p.Spec.MaxReplicas),
		Metrics:     metrics,
		Activation:  activation,
		Scaling:     scaling,
		Selector:    proto.String(selector),
	}.Build()
	if err == nil {
		pol.SetEtag(sp.GetEtag())
		pol.SetRecommenderMetrics(sp.GetRecommenderMetrics())
	}
	if p.Spec.MinReplicas != nil {
		pol.SetMinReplicas(*p.Spec.MinReplicas)
	}

	updatePolicyRequest := pb.UpdatePolicyRequest_builder{
		Policy:       pol,
		AllowMissing: proto.Bool(true),
	}.Build()

	_, err = c.grpcClient.UpdatePolicy(ctx, updatePolicyRequest)
	return err
}

func (c *Controller) enqueuePolicy(obj interface{}) {
	// Handle tombstones from the informer on missed deletes.
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot enqueue object %#v: %w", obj, err))
		return
	}
	c.workqueue.Add(policyID{namespace: accessor.GetNamespace(), name: accessor.GetName()})
}

func (c *Controller) poller() {
	allPolicies, err := c.policiesLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, p := range allPolicies {
		c.workqueue.Add(policyIDOf(p))
	}
}

func (c *Controller) syncWorkload(policy *xasv1.ScalingPolicy, selector string) error {
	// 1. List Pods
	pods, err := c.kubeclientset.CoreV1().Pods(policy.Namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}

	// 2. Build PodStates
	var replicas []*pb.PodState
	for i := range pods.Items {
		replicas = append(replicas, buildPodState(&pods.Items[i]))
	}
	workload := pb.Workload_builder{
		Pods: replicas,
	}.Build()

	// 3. Send
	req := pb.UpdateWorkloadRequest_builder{
		Id:       c.protoPolicyID(policyIDOf(policy)),
		Workload: workload,
	}.Build()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = c.grpcClient.UpdateWorkload(ctx, req)
	return err
}

// buildPodState reports the state of a pod, including the lifecycle facts the
// Server needs to decide whether the pod's metrics should be considered.
func buildPodState(pod *corev1.Pod) *pb.PodState {
	state := pb.PodState_builder{
		Name:       new(pod.Name),
		NodeName:   new(pod.Spec.NodeName),
		Phase:      new(string(pod.Status.Phase)),
		Containers: buildContainerStates(pod),
	}.Build()
	if pod.DeletionTimestamp != nil {
		state.SetDeletionTimestamp(timestamppb.New(pod.DeletionTimestamp.Time))
	}
	if pod.Status.StartTime != nil {
		state.SetStartTime(timestamppb.New(pod.Status.StartTime.Time))
	}
	if cond := podutil.ReadyCondition(pod); cond != nil {
		state.SetIsReady(cond.Status == corev1.ConditionTrue)
		state.SetReadyLastTransitionTime(timestamppb.New(cond.LastTransitionTime.Time))
	}
	return state
}

// buildContainerStates reports the resource requests currently declared for each
// of the pod's containers. Requests are read from the pod spec.
func buildContainerStates(pod *corev1.Pod) []*pb.ContainerState {
	containers := make([]*pb.ContainerState, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		state := pb.ContainerState_builder{
			Name:     proto.String(container.Name),
			Requests: resourceListStrings(container.Resources.Requests),
			Limits:   resourceListStrings(container.Resources.Limits),
		}.Build()
		containers = append(containers, state)
	}
	return containers
}

// resourceListStrings returns the quantities of the list as strings, or nil
// if the list is empty.
func resourceListStrings(list corev1.ResourceList) map[string]string {
	if len(list) == 0 {
		return nil
	}
	out := make(map[string]string, len(list))
	for name, quantity := range list {
		out[string(name)] = quantity.String()
	}
	return out
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
