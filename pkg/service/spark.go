/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
	"github.com/ignotas/spark-operator-apimachinery/pkg/model"
	"github.com/ignotas/spark-operator-apimachinery/pkg/serviceclient"
)

type SparkService interface {
	ListApplications(ctx context.Context, req model.ListRequest) ([]model.SparkApplicationResponse, error)
	CreateApplication(ctx context.Context, req model.CreateRequest) error
	DeleteApplication(ctx context.Context, req model.DeleteRequest) error
	GetApplicationStatus(ctx context.Context, req model.StatusRequest) (*v1beta2.SparkApplicationStatus, error)
}

type sparkService struct {
	client serviceclient.SparkClient
}

func NewSparkService(client serviceclient.SparkClient) SparkService {
	return &sparkService{client: client}
}

func toSparkApplicationResponse(app v1beta2.SparkApplication) model.SparkApplicationResponse {
	return model.SparkApplicationResponse{
		Name:      app.Name,
		Namespace: app.Namespace,
		State:     string(app.Status.AppState.State),
	}
}

func (s *sparkService) ListApplications(ctx context.Context, req model.ListRequest) ([]model.SparkApplicationResponse, error) {
	apps, err := s.client.ListSparkApplications(ctx, req.Namespace)
	if err != nil {
		return nil, err
	}
	asdf, _ := json.Marshal(apps)
	fmt.Println(string(asdf))
	responses := make([]model.SparkApplicationResponse, len(apps))
	for i, app := range apps {
		responses[i] = toSparkApplicationResponse(app)
	}
	return responses, nil
}

func (s *sparkService) CreateApplication(ctx context.Context, req model.CreateRequest) error {

	app := &v1beta2.SparkApplication{
		ObjectMeta: req.Metadata,
		Spec:       req.Spec,
	}
	asdf, _ := json.Marshal(app)
	fmt.Println(string(asdf))

	err := s.client.CreateSparkApplication(ctx, app, req.DeleteIfExists, req.Namespace, req.UploadToPath, req.UploadToEndpoint, req.UploadToRegion, req.S3ForcePathStyle, req.RootPath, req.Override, req.Public)
	if err != nil {
		return err
	}

	return nil
}

func (s *sparkService) DeleteApplication(ctx context.Context, req model.DeleteRequest) error {
	err := s.client.DeleteSparkApplication(ctx, req.Namespace, req.Name)
	if err != nil {
		return err
	}

	return nil
}

func (s *sparkService) GetApplicationStatus(ctx context.Context, req model.StatusRequest) (*v1beta2.SparkApplicationStatus, error) {
	status, err := s.client.GetApplicationStatus(ctx, req.Namespace, req.Name)
	if err != nil {
		return nil, err
	}

	return status, nil
}
