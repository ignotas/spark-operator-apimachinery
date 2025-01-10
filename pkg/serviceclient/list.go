package serviceclient

import (
	"context"

	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *sparkClient) ListSparkApplications(ctx context.Context, namespace string) ([]v1beta2.SparkApplication, error) {
	list, err := c.crdClientSet.SparkoperatorV1beta2().SparkApplications(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return list.Items, nil
}
