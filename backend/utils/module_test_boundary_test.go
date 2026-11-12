package utils

import (
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tidy loads tests of dependency packages too. External utils tests must not
// import controller/service integration pipelines: drift imports utils without
// inheriting backend's Kubernetes replacements. Keep those tests in controllers.
func TestUtilsModuleDependencyTestBoundary(t *testing.T) {
	output, err := exec.Command("go", "list", "-mod=readonly", "-json", ".").Output()
	require.NoError(t, err)
	var pkg struct{ TestImports, XTestImports []string }
	require.NoError(t, json.Unmarshal(output, &pkg))
	for _, imports := range [][]string{pkg.TestImports, pkg.XTestImports} {
		for _, path := range imports {
			require.NotEqual(t, "github.com/diggerhq/digger/backend/controllers", path, "controller integration tests belong in the controllers package")
			require.NotEqual(t, "github.com/diggerhq/digger/backend/services", path, "service integration tests belong in a package that owns their module replacements")
		}
	}
}
