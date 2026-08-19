// envtest-server starts a real kube-apiserver + etcd via envtest, writes the
// KUBECONFIG path to stdout, and blocks until SIGTERM or SIGINT.  This lets
// C++ tests launch it as a subprocess and talk to a real K8s API without a
// full cluster.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func main() {
	env := &envtest.Environment{}

	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest start failed: %v\n", err)
		os.Exit(1)
	}

	// Write a KUBECONFIG file that points at the envtest kube-apiserver.
	kubeconfigPath := filepath.Join(os.TempDir(), fmt.Sprintf("envtest-kubeconfig-%d", os.Getpid()))
	kubeconfig := clientcmdapi.NewConfig()
	kubeconfig.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
	}
	kubeconfig.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
	}
	kubeconfig.Contexts["envtest"] = &clientcmdapi.Context{
		Cluster:  "envtest",
		AuthInfo: "envtest",
	}
	kubeconfig.CurrentContext = "envtest"

	if err := clientcmd.WriteToFile(*kubeconfig, kubeconfigPath); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write kubeconfig: %v\n", err)
		env.Stop()
		os.Exit(1)
	}

	// Print the kubeconfig path — the parent process reads this from stdout.
	fmt.Println(kubeconfigPath)

	// Block until SIGTERM or SIGINT.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	os.Remove(kubeconfigPath)
	env.Stop()
}
