package serviceclient

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *sparkClient) DeleteSparkApplication(ctx context.Context, Namespace string, name string) error {
	return c.crdClientSet.SparkoperatorV1beta2().SparkApplications(Namespace).Delete(ctx, name, metav1.DeleteOptions{})
}
