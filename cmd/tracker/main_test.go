package main

import "testing"

func TestRoleValidation(t *testing.T) {
	roles, err := parseRoles("worker,admin,worker")
	if err != nil || len(roles) != 2 {
		t.Fatalf("roles = %+v, %v", roles, err)
	}
	if _, err := parseRoles("shard_owner"); err == nil {
		t.Fatal("legacy shard role accepted")
	}
}
