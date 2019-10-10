package xf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssignMismatchedTypes(t *testing.T) {
	src := 1
	err := assign("x", &src)
	assert.EqualError(t, err, "fetchable type string is not assignable to recomputed type *int")
}

func TestAssignMismatchedPointersAB(t *testing.T) {
	dest := "x"
	src := "y"
	err := assign(dest, &src)
	assert.EqualError(t, err, "fetchable type string is not assignable to recomputed type *string")
}

func TestAssignMismatchedPointersSrcNotAPointer(t *testing.T) {
	dest := "x"
	src := "y"
	err := assign(&dest, src)
	assert.EqualError(t, err, "recomputed's underlying value type not be a non-nil pointer")
}

func TestStrs(t *testing.T) {
	dest := "y"
	src := "x"
	err := assign(&dest, &src)
	assert.NoError(t, err)
	assert.Equal(t, "x", dest)
}

func TestInts(t *testing.T) {
	dest := 1
	src := 2
	err := assign(&dest, &src)
	assert.NoError(t, err)
	assert.Equal(t, 2, dest)
}

func TestBools(t *testing.T) {
	dest := false
	src := true
	err := assign(&dest, &src)
	assert.NoError(t, err)
	assert.True(t, dest)
}

func TestSlice(t *testing.T) {
	dest := []int{1}
	src := []int{2}
	err := assign(&dest, &src)
	assert.NoError(t, err)
	assert.Equal(t, []int{2}, dest)
}

func TestStruct(t *testing.T) {
	type obj struct {
		field string
	}

	dest := obj{field: "x"}
	src := obj{field: "y"}
	err := assign(&dest, &src)
	assert.NoError(t, err)
	assert.Equal(t, obj{field: "y"}, dest)
}

func TestSliceOfStructs(t *testing.T) {
	type obj struct {
		field string
	}

	dest := []obj{{field: "x"}}
	src := []obj{{field: "y"}}
	err := assign(&dest, &src)
	assert.NoError(t, err)
	assert.Equal(t, []obj{{field: "y"}}, dest)
}
