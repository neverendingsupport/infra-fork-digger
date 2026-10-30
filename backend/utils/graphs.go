package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/dominikbraun/graph"
	"github.com/google/uuid"
)

// ConvertJobsToDiggerJobs jobs is map with project name as a key and a Job as a value
func ConvertJobsToDiggerJobs(jobType scheduler.DiggerCommand, jobReporterType string, vcsType models.DiggerVCSType, organisationId uint, jobsMap map[string]scheduler.Job, projectMap map[string]configuration.Project, projectsGraph graph.Graph[string, configuration.Project], githubInstallationId int64, branch string, prNumber int, repoOwner string, repoName string, repoFullName string, commitSha string, commentId *int64, diggerConfigStr string, gitlabProjectId int, aiSummaryCommentId string, reportTerraformOutput bool, coverAllImpactedProjects bool, VCSConnectionId *uint, batchCheckRunData *CheckRunData, jobsCheckRunIdsMap map[string]CheckRunData, origin *models.GitHubCommandSource) (*uuid.UUID, map[string]*models.DiggerJob, error) {
	slog.Info("Converting jobs to Digger jobs",
		"jobType", jobType,
		"vcsType", vcsType,
		"organisationId", organisationId,
		"jobCount", len(jobsMap),
		slog.Group("repository",
			slog.String("fullName", repoFullName),
			slog.String("owner", repoOwner),
			slog.String("name", repoName),
		),
		"branch", branch,
		"prNumber", prNumber,
	)

	result := make(map[string]*models.DiggerJob)
	organisation, err := models.DB.GetOrganisationById(organisationId)
	if err != nil {
		slog.Error("Failed to get organisation",
			"organisationId", organisationId,
			"error", err,
		)
		return nil, nil, fmt.Errorf("error retrieving organisation")
	}
	organisationName := organisation.Name

	backendHostName := GetPublicBaseURL()

	slog.Debug("Processing jobs", "count", len(jobsMap))
	marshalledJobsMap := map[string][]byte{}
	for projectName, job := range jobsMap {
		jobToken, err := models.DB.CreateDiggerJobToken(organisationId)
		if err != nil {
			slog.Error("Failed to create job token",
				"projectName", projectName,
				"organisationId", organisationId,
				"error", err,
			)
			return nil, nil, fmt.Errorf("error creating job token")
		}

		marshalled, err := json.Marshal(scheduler.JobToJson(job, jobType, organisationName, branch, commitSha, jobToken.Value, backendHostName, projectMap[projectName]))
		if err != nil {
			slog.Error("Failed to marshal job",
				"projectName", projectName,
				"error", err,
			)
			return nil, nil, err
		}
		marshalledJobsMap[job.ProjectName] = marshalled

		slog.Debug("Marshalled job",
			"projectName", job.ProjectName,
			"dataLength", len(marshalled),
		)
	}

	var batchCheckRunId *string = nil
	var batchCheckRunUrl *string = nil
	if batchCheckRunData != nil {
		batchCheckRunId = &batchCheckRunData.Id
		batchCheckRunUrl = &batchCheckRunData.Url
	}
	batch, err := models.DB.CreateDiggerBatch(vcsType, githubInstallationId, repoOwner, repoName, repoFullName, prNumber, diggerConfigStr, branch, jobType, commentId, gitlabProjectId, aiSummaryCommentId, reportTerraformOutput, coverAllImpactedProjects, VCSConnectionId, commitSha, batchCheckRunId, batchCheckRunUrl)
	if err != nil {
		slog.Error("Failed to create batch", "error", err)
		return nil, nil, fmt.Errorf("failed to create batch: %v", err)
	}

	slog.Debug("Created batch", "batchId", batch.ID)
	if origin != nil {
		projects := make([]string, 0, len(jobsMap))
		for project := range jobsMap {
			projects = append(projects, project)
		}
		if err := models.DB.CreateBatchCommandOrigin(batch.ID, *origin, projects); err != nil {
			return nil, nil, fmt.Errorf("record batch command origin: %w", err)
		}
	}

	graphWithImpactedProjectsOnly, err := ImpactedProjectsOnlyGraph(projectsGraph, projectMap)
	if err != nil {
		slog.Error("Failed to create impacted projects graph", "error", err)
		return nil, nil, err
	}

	predecessorMap, err := graphWithImpactedProjectsOnly.PredecessorMap()
	if err != nil {
		slog.Error("Failed to get predecessor map", "error", err)
		return nil, nil, err
	}

	var visitErr error
	visit := func(value string) bool {
		var checkID, checkURL *string
		if check, ok := jobsCheckRunIdsMap[value]; ok {
			checkID, checkURL = &check.Id, &check.Url
		}
		// A project is one job, even when it has several dependencies. Each
		// dependency points to that same child so all parents gate its execution.
		job, err := models.DB.CreateDiggerJob(batch.ID, marshalledJobsMap[value], projectMap[value].WorkflowFile, checkID, checkURL, jobReporterType, value)
		if err != nil {
			visitErr = fmt.Errorf("create project job %s: %w", value, err)
			return true
		}
		if _, err := models.DB.CreateDiggerJobLink(job.DiggerJobID, repoFullName); err != nil {
			visitErr = fmt.Errorf("link project job %s: %w", value, err)
			return true
		}
		for _, edge := range predecessorMap[value] {
			parent := result[edge.Source]
			if parent == nil {
				visitErr = fmt.Errorf("project %s has an uncreated parent %s", value, edge.Source)
				return true
			}
			if err := models.DB.CreateDiggerJobParentLink(parent.DiggerJobID, job.DiggerJobID); err != nil {
				visitErr = fmt.Errorf("link project dependency %s: %w", value, err)
				return true
			}
		}
		result[value] = job
		slog.Debug("Created project job", "projectName", value, "jobId", job.DiggerJobID,
			"batchId", batch.ID, "parentCount", len(predecessorMap[value]))
		return false
	}

	err = TraverseGraphVisitAllParentsFirst(graphWithImpactedProjectsOnly, visit)
	if err != nil {
		slog.Error("Failed to traverse graph", "error", err)
		return nil, nil, err
	}

	if visitErr != nil {
		slog.Error("Failed to create project jobs", "batchId", batch.ID, "error", visitErr)
		return nil, nil, visitErr
	}

	if origin != nil {
		if err := models.DB.SealBatchCommandOrigin(batch.ID); err != nil {
			return nil, nil, fmt.Errorf("verify batch command manifest: %w", err)
		}
	}

	slog.Info("Successfully converted jobs to Digger jobs",
		"batchId", batch.ID,
		"diggerJobCount", len(result),
	)

	return &batch.ID, result, nil
}

// TraverseGraphVisitAllParentsFirst visits each project after every dependency.
// Returning true from visit stops traversal. The input graph is not modified.
func TraverseGraphVisitAllParentsFirst(g graph.Graph[string, configuration.Project], visit func(value string) bool) error {
	order, err := graph.StableTopologicalSort(g, func(a, b string) bool { return a < b })
	if err != nil {
		slog.Error("Failed to order project dependencies", "error", err)
		return err
	}
	for _, project := range order {
		if visit(project) {
			break
		}
	}
	return nil
}

func ImpactedProjectsOnlyGraph(projectsGraph graph.Graph[string, configuration.Project], impactedProjectMap map[string]configuration.Project) (graph.Graph[string, configuration.Project], error) {
	slog.Debug("Creating graph with only impacted projects",
		"totalProjects", len(impactedProjectMap),
	)

	adjacencyMap, err := projectsGraph.AdjacencyMap()
	if err != nil {
		slog.Error("Failed to get adjacency map", "error", err)
		return nil, err
	}

	predecessorMap, err := projectsGraph.PredecessorMap()
	if err != nil {
		slog.Error("Failed to get predecessor map", "error", err)
		return nil, err
	}

	graphWithImpactedProjectsOnly := graph.NewLike(projectsGraph)

	rootCount := 0
	for node := range predecessorMap {
		if predecessorMap[node] == nil || len(predecessorMap[node]) == 0 {
			err := CollapsedGraph(nil, node, adjacencyMap, graphWithImpactedProjectsOnly, impactedProjectMap)
			if err != nil {
				slog.Error("Failed to collapse graph",
					"node", node,
					"error", err,
				)
				return nil, err
			}
			rootCount++
		}
	}

	slog.Debug("Created impacted projects graph",
		"rootNodeCount", rootCount,
	)
	return graphWithImpactedProjectsOnly, nil
}

func CollapsedGraph(impactedParent *string, currentNode string, adjMap map[string]map[string]graph.Edge[string], g graph.Graph[string, configuration.Project], impactedProjects map[string]configuration.Project) error {
	// add to the resulting graph only if the project has been impacted by changes
	if _, ok := impactedProjects[currentNode]; ok {
		currentProject, ok := impactedProjects[currentNode]
		if !ok {
			slog.Error("Project not found", "projectName", currentNode)
			return fmt.Errorf("project %s not found", currentNode)
		}

		err := g.AddVertex(currentProject)
		alreadyVisited := errors.Is(err, graph.ErrVertexAlreadyExists)
		if err != nil && !alreadyVisited {
			slog.Error("Failed to add vertex",
				"projectName", currentNode,
				"error", err,
			)
			return err
		}

		slog.Debug("Added impacted project to graph", "projectName", currentNode)

		// Reaching an existing child through a second parent still adds that
		// dependency. Its descendants have already been visited.
		if !alreadyVisited {
			for child := range adjMap[currentNode] {
				err := CollapsedGraph(&currentNode, child, adjMap, g, impactedProjects)
				if err != nil {
					return err
				}
			}
		}

		// if there is an impacted parent add an edge
		if impactedParent != nil {
			err := g.AddEdge(*impactedParent, currentNode)
			if err != nil && !errors.Is(err, graph.ErrEdgeAlreadyExists) {
				slog.Error("Failed to add edge",
					"parent", *impactedParent,
					"child", currentNode,
					"error", err,
				)
				return err
			}

			slog.Debug("Added edge between impacted projects",
				"parent", *impactedParent,
				"child", currentNode,
			)
		}
	} else {
		// if current wasn't impacted, see children of current node and set currently known parent
		for child := range adjMap[currentNode] {
			err := CollapsedGraph(impactedParent, child, adjMap, g, impactedProjects)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
