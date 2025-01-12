package serviceclient

import (
	"context"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *sparkClient) DeleteSparkApplication(ctx context.Context, Namespace string, name string) error {
	err := c.crdClientSet.SparkoperatorV1beta2().SparkApplications(Namespace).Delete(ctx, name, metav1.DeleteOptions{})

	if err != nil && strings.Contains(err.Error(), "not found") {
		return nil
	}

	return err
}
