package utils

import (
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"reflect"
	"testing"
)

func TestImpactedProjectsOnlyGraph(t *testing.T) {
	p1 := configuration.Project{Name: "p1"}
	p2 := configuration.Project{Name: "p2", DependencyProjects: []string{"p1"}}
	p3 := configuration.Project{Name: "p3", DependencyProjects: []string{"p1", "p2"}}
	p4 := configuration.Project{Name: "p4"}
	p5 := configuration.Project{Name: "p5", DependencyProjects: []string{"p4"}}
	p6 := configuration.Project{Name: "p6", DependencyProjects: []string{"p1", "p2", "p3"}}
	projects := []configuration.Project{p1, p2, p3, p4, p5, p6}

	impactedProjects := map[string]configuration.Project{"p1": p1, "p2": p2, "p3": p3}

	dg, err := configuration.CreateProjectDependencyGraph(projects)

	if err != nil {
		t.Errorf("Error creating dependency graph: %v", err)
	}

	newGraph, err := ImpactedProjectsOnlyGraph(dg, impactedProjects)

	if err != nil {
		t.Errorf("Error creating impacted projects only graph: %v", err)
	}

	adjMap, err := newGraph.AdjacencyMap()

	if err != nil {
		t.Errorf("Error getting adjacency map: %v", err)
	}

	if len(adjMap) != 3 {
		t.Errorf("Expected 3 projects, got %d", len(adjMap))
	}

	if _, ok := adjMap["p1"]; !ok {
		t.Errorf("Expected p1 to be in the graph")
	}

	if _, ok := adjMap["p2"]; !ok {
		t.Errorf("Expected p2 to be in the graph")
	}

	if _, ok := adjMap["p3"]; !ok {
		t.Errorf("Expected p3 to be in the graph")
	}
}

func TestImpactedProjectsOnlyGraph2(t *testing.T) {
	p1 := configuration.Project{Name: "p1"}
	p2 := configuration.Project{Name: "p2", DependencyProjects: []string{"p1"}}
	p3 := configuration.Project{Name: "p3", DependencyProjects: []string{"p1", "p2"}}
	p4 := configuration.Project{Name: "p4"}
	p5 := configuration.Project{Name: "p5", DependencyProjects: []string{"p4"}}
	p6 := configuration.Project{Name: "p6", DependencyProjects: []string{"p1", "p2", "p3"}}
	projects := []configuration.Project{p1, p2, p3, p4, p5, p6}

	impactedProjects := map[string]configuration.Project{"p1": p1, "p2": p2, "p3": p3, "p5": p5}

	dg, err := configuration.CreateProjectDependencyGraph(projects)

	if err != nil {
		t.Errorf("Error creating dependency graph: %v", err)
	}

	newGraph, err := ImpactedProjectsOnlyGraph(dg, impactedProjects)

	if err != nil {
		t.Errorf("Error creating impacted projects only graph: %v", err)
	}

	adjMap, err := newGraph.AdjacencyMap()

	if err != nil {
		t.Errorf("Error getting adjacency map: %v", err)
	}

	if len(adjMap) != 4 {
		t.Errorf("Expected 4 projects, got %d", len(adjMap))
	}

	if _, ok := adjMap["p1"]; !ok {
		t.Errorf("Expected p1 to be in the graph")
	}

	if _, ok := adjMap["p2"]; !ok {
		t.Errorf("Expected p2 to be in the graph")
	}

	if _, ok := adjMap["p3"]; !ok {
		t.Errorf("Expected p3 to be in the graph")
	}

	if _, ok := adjMap["p5"]; !ok {
		t.Errorf("Expected p5 to be in the graph")
	}
}

func TestImpactedProjectsOnlyGraph3(t *testing.T) {
	p1 := configuration.Project{Name: "p1"}
	p2 := configuration.Project{Name: "p2", DependencyProjects: []string{"p1"}}
	p3 := configuration.Project{Name: "p3", DependencyProjects: []string{"p1", "p2"}}
	p4 := configuration.Project{Name: "p4"}
	p5 := configuration.Project{Name: "p5", DependencyProjects: []string{"p4"}}
	p6 := configuration.Project{Name: "p6", DependencyProjects: []string{"p1", "p2", "p3"}}
	projects := []configuration.Project{p1, p2, p3, p4, p5, p6}

	impactedProjects := map[string]configuration.Project{"p1": p1, "p3": p3, "p6": p6}

	dg, err := configuration.CreateProjectDependencyGraph(projects)

	if err != nil {
		t.Errorf("Error creating dependency graph: %v", err)
	}

	newGraph, err := ImpactedProjectsOnlyGraph(dg, impactedProjects)

	if err != nil {
		t.Errorf("Error creating impacted projects only graph: %v", err)
	}

	adjMap, err := newGraph.AdjacencyMap()

	if err != nil {
		t.Errorf("Error getting adjacency map: %v", err)
	}

	if len(adjMap) != 3 {
		t.Errorf("Expected 3 projects, got %d", len(adjMap))
	}

	if _, ok := adjMap["p1"]; !ok {
		t.Errorf("Expected p1 to be in the graph")
	}

	if _, ok := adjMap["p3"]; !ok {
		t.Errorf("Expected p3 to be in the graph")
	}

	if _, ok := adjMap["p6"]; !ok {
		t.Errorf("Expected p5 to be in the graph")
	}
}

func TestTraverseGraphVisitAllParentsFirst(t *testing.T) {
	projects := []configuration.Project{
		{Name: "a"}, {Name: "isolated"},
		{Name: "b", DependencyProjects: []string{"a"}},
		{Name: "d", DependencyProjects: []string{"b"}},
		{Name: "c", DependencyProjects: []string{"a", "d"}},
	}
	dg, err := configuration.CreateProjectDependencyGraph(projects)
	if err != nil {
		t.Fatal(err)
	}
	before, err := dg.AdjacencyMap()
	if err != nil {
		t.Fatal(err)
	}
	visited := make(map[string]int)
	err = TraverseGraphVisitAllParentsFirst(dg, func(value string) bool {
		if _, found := visited[value]; found {
			t.Fatalf("visited %s twice", value)
		}
		visited[value] = len(visited)
		return false
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(visited) != len(projects) {
		t.Fatalf("visited %v, expected all %d projects", visited, len(projects))
	}
	for _, project := range projects {
		for _, parent := range project.DependencyProjects {
			if visited[parent] >= visited[project.Name] {
				t.Errorf("parent %s must precede %s: %v", parent, project.Name, visited)
			}
		}
	}
	after, err := dg.AdjacencyMap()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("traversal changed input graph")
	}
	calls := 0
	if err := TraverseGraphVisitAllParentsFirst(dg, func(string) bool { calls++; return true }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("stop visitor called %d times", calls)
	}
}
