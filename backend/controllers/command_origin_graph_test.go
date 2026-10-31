package controllers

import (
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestCommandOriginConvergingProjectsHaveOneJobEach(t *testing.T) {
	for _, depth := range []string{"equal", "unequal"} {
		for _, capture := range []bool{false, true} {
			name := "legacy"
			if capture {
				name = "with_origin"
			}
			t.Run(depth+"/"+name, func(t *testing.T) {
				event := commandOriginEvent()
				event.Repo.FullName = github.String("owner/repo")
				event.Installation = &github.Installation{ID: github.Int64(77)}
				db, org, _ := commandPipelineDB(t, event)
				projects := []configuration.Project{{Name: "a"}, {Name: "b"}, {Name: "c", DependencyProjects: []string{"a", "b"}}}
				if depth == "unequal" {
					projects = []configuration.Project{{Name: "a"}, {Name: "b", DependencyProjects: []string{"a"}}, {Name: "d", DependencyProjects: []string{"b"}}, {Name: "c", DependencyProjects: []string{"a", "d"}}}
				}
				graph, err := configuration.CreateProjectDependencyGraph(projects)
				require.NoError(t, err)
				jobs := map[string]scheduler.Job{}
				projectMap := map[string]configuration.Project{}
				checks := map[string]utils.CheckRunData{}
				for _, p := range projects {
					projectMap[p.Name] = p
					jobs[p.Name] = scheduler.Job{ProjectName: p.Name, Namespace: "owner/repo", PullRequestNumber: github.Int(9)}
					checks[p.Name] = utils.CheckRunData{Id: "check-" + p.Name}
				}
				var source *models.GitHubCommandSource
				if capture {
					source, err = githubCommandSource(event)
					require.NoError(t, err)
				}
				batchID, result, err := utils.ConvertJobsToDiggerJobs(scheduler.DiggerCommandPlan, "lazy", models.DiggerVCSGithub, org.ID,
					jobs, projectMap, graph, 77, "feature", 9, "owner", "repo", "owner/repo", "head", github.Int64(999), "", 0, "", false, true, nil,
					&utils.CheckRunData{Id: "aggregate"}, checks, source)
				require.NoError(t, err)
				require.Len(t, result, len(projects))
				var stored []models.DiggerJob
				require.NoError(t, db.GormDB.Where("batch_id = ?", batchID.String()).Find(&stored).Error)
				require.Len(t, stored, len(projects), "a child with two parents is still one job")
				links, err := db.GetDiggerJobParentLinksChildId(&result["c"].DiggerJobID)
				require.NoError(t, err)
				require.Len(t, links, 2)
				if capture {
					got, err := db.GetCommandProvenance(org.ID, "owner/repo", 9, event.Comment.GetID())
					require.NoError(t, err)
					require.Len(t, got.Batches, 1)
					require.True(t, got.Batches[0].ManifestComplete)
				}
			})
		}
	}
}
