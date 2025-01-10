package serviceclient

import (
	"context"

	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
)

func (s *sparkClient) GetApplicationStatus(ctx context.Context, namespace string, name string) (*v1beta2.SparkApplicationStatus, error) {
	app, err := s.getSparkApplication(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	return &app.Status, nil
}
