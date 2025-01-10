package serviceclient

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
)

func (s *sparkClient) getSparkApplication(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplication, error) {
	app, err := s.crdClientSet.SparkoperatorV1beta2().SparkApplications(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return app, nil
}
