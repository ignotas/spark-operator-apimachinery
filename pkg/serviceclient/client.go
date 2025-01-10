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

package serviceclient

import (
	"context"

	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
	crdclientset "github.com/ignotas/spark-operator-apimachinery/pkg/client/clientset/versioned"
	clientset "k8s.io/client-go/kubernetes"
)

type SparkClient interface {
	ListSparkApplications(ctx context.Context, namespace string) ([]v1beta2.SparkApplication, error)
	CreateSparkApplication(ctx context.Context, app *v1beta2.SparkApplication, DeleteIfExists bool, Namespace string, UploadToPath string, UploadToEndpoint string, UploadToRegion string, S3ForcePathStyle bool, RootPath string, Override bool, Public bool) error
	DeleteSparkApplication(ctx context.Context, Namespace string, name string) error
	GetApplicationStatus(ctx context.Context, Namespace string, name string) (*v1beta2.SparkApplicationStatus, error)
}

type sparkClient struct {
	kubeClient   clientset.Interface
	crdClientSet crdclientset.Interface
}

func NewSparkClient(kubeClient clientset.Interface, crdClientSet crdclientset.Interface) SparkClient {
	return &sparkClient{
		kubeClient:   kubeClient,
		crdClientSet: crdClientSet,
	}
}
