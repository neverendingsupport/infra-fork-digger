-- Create "batch_command_origins" table
CREATE TABLE "public"."batch_command_origins" (
  "batch_id" uuid NOT NULL,
  "repository_id" bigint NOT NULL,
  "repo_full_name" text NOT NULL,
  "pr_number" bigint NOT NULL,
  "source_comment_id" bigint NOT NULL,
  "body_sha256" character varying(64) NOT NULL,
  "commit_sha" text NOT NULL,
  "command" text NOT NULL,
  "github_installation_id" bigint NOT NULL,
  "aggregate_check_id" text NOT NULL,
  "expected_projects" text NOT NULL,
  "job_manifest" text NULL,
  "sealed" boolean NOT NULL DEFAULT false,
  PRIMARY KEY ("batch_id")
);
-- Create index "idx_command_origin_source" to table: "batch_command_origins"
CREATE INDEX "idx_command_origin_source" ON "public"."batch_command_origins" ("repo_full_name", "pr_number", "source_comment_id");
-- Modify "digger_jobs" table
ALTER TABLE "public"."digger_jobs" ADD COLUMN "workflow_run_id" bigint NULL, ADD COLUMN "workflow_run_attempt" bigint NULL;
