// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import context "context"
import mock "github.com/stretchr/testify/mock"
import time "time"
import xf "github.com/Onefootball/xfetch-go"

// Cache is an autogenerated mock type for the Cache type
type Cache struct {
	mock.Mock
}

// Read provides a mock function with given fields: ctx, key, fetchable
func (_m *Cache) Read(ctx context.Context, key string, fetchable xf.Fetchable) (float64, float64, error) {
	ret := _m.Called(ctx, key, fetchable)

	var r0 float64
	if rf, ok := ret.Get(0).(func(context.Context, string, xf.Fetchable) float64); ok {
		r0 = rf(ctx, key, fetchable)
	} else {
		r0 = ret.Get(0).(float64)
	}

	var r1 float64
	if rf, ok := ret.Get(1).(func(context.Context, string, xf.Fetchable) float64); ok {
		r1 = rf(ctx, key, fetchable)
	} else {
		r1 = ret.Get(1).(float64)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string, xf.Fetchable) error); ok {
		r2 = rf(ctx, key, fetchable)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// Update provides a mock function with given fields: ctx, key, ttl, delta, fetchable
func (_m *Cache) Update(ctx context.Context, key string, ttl time.Duration, delta float64, fetchable xf.Fetchable) error {
	ret := _m.Called(ctx, key, ttl, delta, fetchable)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, time.Duration, float64, xf.Fetchable) error); ok {
		r0 = rf(ctx, key, ttl, delta, fetchable)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
