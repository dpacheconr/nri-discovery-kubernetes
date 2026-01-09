package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/newrelic/nri-discovery-kubernetes/internal/config"
	"github.com/newrelic/nri-discovery-kubernetes/internal/discovery"
	"github.com/newrelic/nri-discovery-kubernetes/internal/http"
	kubelet "github.com/newrelic/nri-discovery-kubernetes/internal/kubernetes"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/homedir"
)

// Version of the integration.
var integrationVersion = "dev"

const (
	exitKubernetesConfigurationReadError = iota + 1
	exitNoConnectionToKubelet
	exitJSONMarchallError
	exitKubernetesConfigurationBuildError
	exitKubernetesClientBuildError
	exitKubeletClientBuildError
)

func main() {
	cfg, err := config.NewConfig(integrationVersion)
	if err != nil {
		log.Printf("failed read the configuration: %s ", err)
		os.Exit(exitKubernetesConfigurationReadError)
	}

	k8sConfig, err := getK8sConfig(cfg)
	if err != nil {
		log.Printf("setting kubernetes configuration: %s", err)
		os.Exit(exitKubernetesConfigurationBuildError)
	}

	k8sClientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		log.Printf("building kubernetes client: %s", err)
		os.Exit(exitKubernetesClientBuildError)
	}

	if cfg.EnableLeaderElection {
		runWithLeaderElection(context.Background(), cfg, k8sClientset, k8sConfig)
	} else {
		runDiscoveryOnce(cfg, k8sClientset, k8sConfig)
	}
}

func runDiscoveryOnce(cfg *config.Config, k8s *kubernetes.Clientset, k8sConfig *rest.Config) {
	connector := http.DefaultConnector(k8s, cfg, k8sConfig, log.New())

	httpClient, err := http.NewClient(connector, http.WithMaxRetries(cfg.Retries))
	if err != nil {
		log.Printf("building kubelet client: %s", err)
		os.Exit(exitKubeletClientBuildError)
	}

	kube := kubelet.New(httpClient, cfg)
	discoverer := discovery.NewDiscoverer(cfg.Namespaces, kube, cfg.DiscoverServices)

	// If discovering services, initialize and set the service discoverer
	if cfg.DiscoverServices {
		serviceDiscoverer := kubelet.NewServiceDiscoverer(k8s, cfg)
		discoverer.SetServiceDiscoverer(serviceDiscoverer)
	}

	output, err := discoverer.Run()
	if err != nil {
		log.Printf("failed to connect to Kubernetes: %s", err)
		os.Exit(exitNoConnectionToKubelet)
	}

	bytes, err := json.Marshal(output)
	if err != nil {
		log.Printf("failed to marshal result to Json: %s", err)
		os.Exit(exitJSONMarchallError)
	}
	fmt.Println(string(bytes))
}

func runWithLeaderElection(ctx context.Context, cfg *config.Config, k8s *kubernetes.Clientset, k8sConfig *rest.Config) {
	// Validate leader election configuration
	if cfg.PodName == "" {
		log.Printf("POD_NAME environment variable not set, required for leader election")
		os.Exit(exitKubernetesConfigurationReadError)
	}
	if cfg.LeaderElectionNamespace == "" {
		log.Printf("POD_NAMESPACE environment variable not set, required for leader election")
		os.Exit(exitKubernetesConfigurationReadError)
	}

	// Create resource lock for leader election using Lease
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaderElectionLeaseName,
			Namespace: cfg.LeaderElectionNamespace,
		},
		Client: k8s.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.PodName,
		},
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("Received shutdown signal")
		cancel()
	}()

	// Run leader election
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   cfg.LeaderElectionLeaseDuration,
		RenewDeadline:   cfg.LeaderElectionRenewDeadline,
		RetryPeriod:     cfg.LeaderElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("Became leader, starting discovery")
				runDiscoveryOnce(cfg, k8s, k8sConfig)
			},
			OnStoppedLeading: func() {
				log.Info("Lost leadership, shutting down")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity != cfg.PodName {
					log.Infof("New leader elected: %s", identity)
				}
			},
		},
	})
}

func getK8sConfig(c *config.Config) (*rest.Config, error) {
	inclusterConfig, err := rest.InClusterConfig()
	if err == nil {
		return inclusterConfig, nil
	}
	log.Warnf("collecting in cluster config: %v", err)

	kubeconf := c.KubeConfigFile
	if kubeconf == "" {
		kubeconf = path.Join(homedir.HomeDir(), ".kube", "config")
	}

	inclusterConfig, err = clientcmd.BuildConfigFromFlags("", kubeconf)
	if err != nil {
		return nil, fmt.Errorf("could not load local kube config: %w", err)
	}

	log.Warnf("using local kube config: %q", kubeconf)

	return inclusterConfig, nil
}
