// Code generated by k8s code-generator DO NOT EDIT.

/*
Copyright 2018 Google LLC

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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1beta1 "github.com/ignotas/spark-operator-apimachinery/api/v1beta1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeScheduledSparkApplications implements ScheduledSparkApplicationInterface
type FakeScheduledSparkApplications struct {
	Fake *FakeSparkoperatorV1beta1
	ns   string
}

var scheduledsparkapplicationsResource = schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta1", Resource: "scheduledsparkapplications"}

var scheduledsparkapplicationsKind = schema.GroupVersionKind{Group: "sparkoperator.k8s.io", Version: "v1beta1", Kind: "ScheduledSparkApplication"}

// Get takes name of the scheduledSparkApplication, and returns the corresponding scheduledSparkApplication object, and an error if there is any.
func (c *FakeScheduledSparkApplications) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.ScheduledSparkApplication, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(scheduledsparkapplicationsResource, c.ns, name), &v1beta1.ScheduledSparkApplication{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.ScheduledSparkApplication), err
}

// List takes label and field selectors, and returns the list of ScheduledSparkApplications that match those selectors.
func (c *FakeScheduledSparkApplications) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.ScheduledSparkApplicationList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(scheduledsparkapplicationsResource, scheduledsparkapplicationsKind, c.ns, opts), &v1beta1.ScheduledSparkApplicationList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1beta1.ScheduledSparkApplicationList{ListMeta: obj.(*v1beta1.ScheduledSparkApplicationList).ListMeta}
	for _, item := range obj.(*v1beta1.ScheduledSparkApplicationList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested scheduledSparkApplications.
func (c *FakeScheduledSparkApplications) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(scheduledsparkapplicationsResource, c.ns, opts))

}

// Create takes the representation of a scheduledSparkApplication and creates it.  Returns the server's representation of the scheduledSparkApplication, and an error, if there is any.
func (c *FakeScheduledSparkApplications) Create(ctx context.Context, scheduledSparkApplication *v1beta1.ScheduledSparkApplication, opts v1.CreateOptions) (result *v1beta1.ScheduledSparkApplication, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(scheduledsparkapplicationsResource, c.ns, scheduledSparkApplication), &v1beta1.ScheduledSparkApplication{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.ScheduledSparkApplication), err
}

// Update takes the representation of a scheduledSparkApplication and updates it. Returns the server's representation of the scheduledSparkApplication, and an error, if there is any.
func (c *FakeScheduledSparkApplications) Update(ctx context.Context, scheduledSparkApplication *v1beta1.ScheduledSparkApplication, opts v1.UpdateOptions) (result *v1beta1.ScheduledSparkApplication, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(scheduledsparkapplicationsResource, c.ns, scheduledSparkApplication), &v1beta1.ScheduledSparkApplication{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.ScheduledSparkApplication), err
}

// Delete takes name of the scheduledSparkApplication and deletes it. Returns an error if one occurs.
func (c *FakeScheduledSparkApplications) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(scheduledsparkapplicationsResource, c.ns, name), &v1beta1.ScheduledSparkApplication{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeScheduledSparkApplications) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(scheduledsparkapplicationsResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1beta1.ScheduledSparkApplicationList{})
	return err
}

// Patch applies the patch and returns the patched scheduledSparkApplication.
func (c *FakeScheduledSparkApplications) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.ScheduledSparkApplication, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(scheduledsparkapplicationsResource, c.ns, name, pt, data, subresources...), &v1beta1.ScheduledSparkApplication{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.ScheduledSparkApplication), err
}
