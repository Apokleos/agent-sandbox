// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import "testing"

func TestAutoSuspensionRequiresScopedAuthorization(t *testing.T) {
	c := Defaults()
	c.EnableAutoSuspension = true
	for _, mode := range []AuthzMode{AuthzAllowAll, AuthzTokenReview} {
		c.AuthzMode = mode
		if c.Validate() == nil {
			t.Fatalf("activity accepted authz mode %q", mode)
		}
	}
	c.AuthzMode = AuthzScopedToken
	c.AuthzScopedTokenSecretFile = "/test/secret"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
