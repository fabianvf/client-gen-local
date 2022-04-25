package main

import (
	"fmt"
	"strings"

	"github.com/kcp-dev/apimachinery/pkg/logicalcluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/informers/core"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	byWorkspaceIndexName             = "byWorkspace"
	byWorkspaceAndNamespaceIndexName = "byWorkspaceAndNamespace"
)

func ClusterAwareKeyFunc(obj interface{}) (string, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("object has no meta: %v", err)
	}
	clusterName := meta.GetZZZ_DeprecatedClusterName()
	namespace := meta.GetNamespace()
	name := meta.GetName()

	return strings.Join([]string{clusterName, namespace, name}, "/"), nil
}

func byWorkspaceIndexFunc(obj interface{}) ([]string, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return []string{""}, fmt.Errorf("object has no meta: %v", err)
	}
	return []string{meta.GetZZZ_DeprecatedClusterName()}, nil
}

func byWorkspaceAndNamespaceIndexFunc(obj interface{}) ([]string, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return []string{""}, fmt.Errorf("object has no meta: %v", err)
	}
	return []string{meta.GetZZZ_DeprecatedClusterName() + "/" + meta.GetNamespace()}, nil
}

type ClusterSharedInformerFactory interface {
	Core() clustercoreInterface
}

func NewClusterSharedInformerFactory(client kubernetes.Interface) *realClusterSharedInformerFactory {
	delegate := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // defaultResync,
		informers.WithExtraClusterScopedIndexers(
			cache.Indexers{
				"byWorkspace": byWorkspaceIndexFunc,
			},
		),
		informers.WithExtraNamespaceScopedIndexers(
			cache.Indexers{
				byWorkspaceIndexName:             byWorkspaceIndexFunc,
				byWorkspaceAndNamespaceIndexName: byWorkspaceAndNamespaceIndexFunc,
			},
		),
		informers.WithKeyFunction(ClusterAwareKeyFunc),
	)

	return &realClusterSharedInformerFactory{
		delegate: delegate,
	}
}

type realClusterSharedInformerFactory struct {
	delegate informers.SharedInformerFactory
}

func (r realClusterSharedInformerFactory) Core() clustercoreInterface {
	return &realclustercoreInterface{
		delegate: r.delegate.Core(),
	}
}

type clustercoreInterface interface {
	V1() clustercorev1Interface
}

type realclustercoreInterface struct {
	delegate core.Interface
}

func (r realclustercoreInterface) V1() clustercorev1Interface {
	return &realclustercorev1Interface{
		delegate: r.delegate.V1(),
	}
}

type clustercorev1Interface interface {
	ConfigMaps() clusterConfigMapInformer
}

type realclustercorev1Interface struct {
	delegate corev1informers.Interface
}

func (r realclustercorev1Interface) ConfigMaps() clusterConfigMapInformer {
	return &realclusterConfigMapInformer{
		delegate: r.delegate.ConfigMaps(),
	}
}

type clusterConfigMapInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() clusterConfigMapLister
}

type realclusterConfigMapInformer struct {
	delegate corev1informers.ConfigMapInformer
}

func (r realclusterConfigMapInformer) Informer() cache.SharedIndexInformer {
	return r.delegate.Informer()
}

func (r realclusterConfigMapInformer) Lister() clusterConfigMapLister {
	return &realclusterConfigMapLister{
		indexer: r.delegate.Informer().GetIndexer(),
	}
}

type clusterConfigMapLister interface {
	List(selector labels.Selector) ([]*corev1.ConfigMap, error)
	Cluster(c logicalcluster.LogicalCluster) corev1listers.ConfigMapLister
}

type realclusterConfigMapLister struct {
	indexer cache.Indexer
}

func (r realclusterConfigMapLister) List(selector labels.Selector) (ret []*corev1.ConfigMap, err error) {
	err = cache.ListAll(r.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*corev1.ConfigMap))
	})
	return ret, err
}

func (r realclusterConfigMapLister) Cluster(c logicalcluster.LogicalCluster) corev1listers.ConfigMapLister {
	return &clusterScopedConfigMapLister{
		cluster:                        c,
		indexer:                        r.indexer,
		byClusterIndexName:             byWorkspaceIndexName,
		byClusterAndNamespaceIndexName: byWorkspaceAndNamespaceIndexName,
	}
}

type clusterScopedConfigMapLister struct {
	cluster                        logicalcluster.LogicalCluster
	indexer                        cache.Indexer
	byClusterIndexName             string
	byClusterAndNamespaceIndexName string
}

func (c clusterScopedConfigMapLister) List(selector labels.Selector) (ret []*corev1.ConfigMap, err error) {
	list, err := c.indexer.ByIndex(c.byClusterIndexName, c.cluster.String())
	if err != nil {
		return nil, err
	}
	for i := range list {
		// TODO: use the selector
		ret = append(ret, list[i].(*corev1.ConfigMap))
	}
	return
}

func (c clusterScopedConfigMapLister) ConfigMaps(namespace string) corev1listers.ConfigMapNamespaceLister {
	return &clusterConfigMapNamespaceLister{
		cluster:                        c.cluster,
		namespace:                      namespace,
		indexer:                        c.indexer,
		byClusterAndNamespaceIndexName: c.byClusterAndNamespaceIndexName,
	}
}

type clusterConfigMapNamespaceLister struct {
	cluster                        logicalcluster.LogicalCluster
	namespace                      string
	indexer                        cache.Indexer
	byClusterAndNamespaceIndexName string
}

func (c clusterConfigMapNamespaceLister) List(selector labels.Selector) (ret []*corev1.ConfigMap, err error) {
	list, err := c.indexer.Index(c.byClusterAndNamespaceIndexName, &metav1.ObjectMeta{
		ZZZ_DeprecatedClusterName: c.cluster.String(),
		Namespace:                 c.namespace,
	})
	if err != nil {
		return nil, err
	}
	for i := range list {
		// TODO: use the selector
		ret = append(ret, list[i].(*corev1.ConfigMap))
	}
	return
}

func (c clusterConfigMapNamespaceLister) Get(name string) (*corev1.ConfigMap, error) {
	meta := &metav1.ObjectMeta{
		ZZZ_DeprecatedClusterName: c.cluster.String(),
		Namespace:                 c.namespace,
		Name:                      name,
	}
	obj, exists, err := c.indexer.Get(meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(corev1.Resource("configmaps"), name)
	}
	return obj.(*corev1.ConfigMap), nil
}
