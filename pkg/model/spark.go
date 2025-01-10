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

package model

import (
	"github.com/ignotas/spark-operator-apimachinery/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ListRequest struct {
	Namespace string `json:"namespace"`
}

type SparkApplicationResponse struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	State     string `json:"state"`
}

type LocalDependencies struct {
	UploadToPath     string `json:"uploadToPath,omitempty"`
	RootPath         string `json:"rootPath,omitempty"`
	UploadToRegion   string `json:"uploadToRegion,omitempty"`
	UploadToEndpoint string `json:"uploadToEndpoint,omitempty"`
	Public           bool   `json:"public,omitempty"`
	S3ForcePathStyle bool   `json:"s3ForcePathStyle,omitempty"`
	Override         bool   `json:"override,omitempty"`
}

type CreateRequest struct {
	Namespace         string                       `json:"namespace,omitempty"`
	DeleteIfExists    bool                         `json:"deleteIfExists,omitempty"`
	LocalDependencies LocalDependencies            `json:"localDependencies,omitempty"`
	Metadata          metav1.ObjectMeta            `json:"metadata"`
	Spec              v1beta2.SparkApplicationSpec `json:"spec"`
}

type DeleteRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type StatusRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
