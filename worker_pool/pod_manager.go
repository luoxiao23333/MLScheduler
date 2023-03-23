package worker_pool

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/metrics/pkg/client/clientset/versioned"
	"log"
	"time"
)

var containerMap = map[string]string{
	"mcmot": "docker.io/luoxiao23333/task_mcmot:v0",
	"slam":  "docker.io/luoxiao23333/task_slam:v0",
}

func CreatePod(taskName, nodeName, hostname string) {
	// create the Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Panic(err)
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Panic(err)
	}

	worker := AddWorker(hostname, taskName)
	name := fmt.Sprintf("%v_%v", taskName, worker.port)
	worker.podName = name

	// define the container
	container := corev1.Container{
		Name:  name,
		Image: containerMap[taskName],
	}

	// define the pod
	pod := &corev1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"worker": taskName,
			},
		},
		Spec: corev1.PodSpec{
			Containers:  []corev1.Container{container},
			HostNetwork: true,
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": nodeName,
			},
		},
	}

	// create the pod
	result, err := clientSet.CoreV1().Pods("default").Create(context.Background(), pod, meta_v1.CreateOptions{})
	if err != nil {
		panic(err.Error())
	}

	log.Printf("Creating pod %v in %v.\n", result.GetObjectMeta().GetName(), nodeName)

	// wait until pod created
	err = wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
		pod, err = clientSet.CoreV1().Pods("default").Get(context.Background(),
			pod.Name, meta_v1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodRunning {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Pod Created!")
}

// ResourceUsage
// Storage 和持久卷绑定，pod删除不消失
// StorageEphemeral pod删除就释放
// Measure resource in range [ CollectedTime - Window, CollectedTime ]
type ResourceUsage struct {
	CPU              int64  `json:"CPU"`
	Memory           int64  `json:"Memory"`
	Storage          int64  `json:"Storage"`
	StorageEphemeral int64  `json:"StorageEphemeral"`
	CollectedTime    string `json:"CollectedTime"`
	Window           int64  `json:"Window"`
}

func QueryResourceUsage(podName string) ResourceUsage {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Panic(err)
	}

	metricsClient, err := versioned.NewForConfig(config)
	if err != nil {
		log.Panic(err)
	}
	metricsInterface := metricsClient.MetricsV1beta1().PodMetricses("default")

	podMetrics, err := metricsInterface.Get(context.Background(), podName, meta_v1.GetOptions{})
	if err != nil {
		log.Panic(err)
	}

	usage := podMetrics.Containers[0].Usage
	return ResourceUsage{
		CPU:              usage.Cpu().MilliValue(),
		Memory:           usage.Memory().MilliValue(),
		Storage:          usage.Storage().MilliValue(),
		StorageEphemeral: usage.StorageEphemeral().MilliValue(),
		CollectedTime:    podMetrics.Timestamp.Time.String(),
		Window:           podMetrics.Window.Duration.Milliseconds(),
	}
}
