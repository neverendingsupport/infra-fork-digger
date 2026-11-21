-- Each GitHub apply comment either reached batch creation or was refused before
-- it, whichever is written first; the primary key serializes the race. A
-- refusal carries its receipt, and only a refusal has one.
CREATE TABLE "public"."command_source_dispositions" (
  "repository_id" bigint NOT NULL,
  "source_comment_id" bigint NOT NULL,
  "disposition" text NOT NULL CHECK ("disposition" IN ('batch_created', 'rejected')),
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "receipt_version" integer NULL CHECK ("receipt_version" = 1),
  "reason" text NULL CHECK ("reason" IN ('apply_requirements', 'project_locked', 'duplicate_command', 'impacted_projects_limit', 'draft_pr')),
  "repo_full_name" text NULL,
  "pr_number" bigint NULL,
  "body_sha256" character varying(64) NULL,
  "commit_sha" text NULL,
  "github_app_id" bigint NULL,
  "github_installation_id" bigint NULL,
  PRIMARY KEY ("repository_id", "source_comment_id"),
  CHECK (CASE "disposition"
    WHEN 'rejected' THEN num_nulls("receipt_version", "reason", "repo_full_name", "pr_number", "body_sha256", "commit_sha", "github_app_id", "github_installation_id") = 0
    ELSE num_nonnulls("receipt_version", "reason", "repo_full_name", "pr_number", "body_sha256", "commit_sha", "github_app_id", "github_installation_id") = 0
  END)
);
CREATE INDEX "idx_command_rejection_source" ON "public"."command_source_dispositions" ("repo_full_name", "pr_number", "source_comment_id")
  WHERE "disposition" = 'rejected';

CREATE FUNCTION "public"."protect_command_source_disposition"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'command source dispositions are immutable';
END;
$$;
CREATE TRIGGER "protect_command_source_disposition" BEFORE UPDATE OR DELETE ON "public"."command_source_dispositions"
  FOR EACH ROW EXECUTE FUNCTION "public"."protect_command_source_disposition"();
CREATE TRIGGER "protect_command_source_disposition_truncate" BEFORE TRUNCATE ON "public"."command_source_dispositions"
  FOR EACH STATEMENT EXECUTE FUNCTION "public"."protect_command_source_disposition"();

-- Origins precede job creation, so refusing one keeps a rejected comment from
-- ever dispatching. A trigger also binds replicas that predate this migration.
-- DG058 lets the backend tell this refusal from other database errors.
CREATE FUNCTION "public"."fence_rejected_command_origin"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO "public"."command_source_dispositions" ("repository_id", "source_comment_id", "disposition")
    VALUES (NEW.repository_id, NEW.source_comment_id, 'batch_created')
    ON CONFLICT DO NOTHING;
  IF EXISTS (SELECT 1 FROM "public"."command_source_dispositions"
             WHERE repository_id = NEW.repository_id AND source_comment_id = NEW.source_comment_id
               AND disposition = 'rejected') THEN
    RAISE EXCEPTION USING ERRCODE = 'DG058',
      MESSAGE = format('command source %s/%s was rejected before dispatch', NEW.repository_id, NEW.source_comment_id);
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER "fence_rejected_command_origin" BEFORE INSERT ON "public"."batch_command_origins"
  FOR EACH ROW WHEN (NEW.command = 'apply') EXECUTE FUNCTION "public"."fence_rejected_command_origin"();

-- Apply comments that already have an origin can never be rejected.
INSERT INTO "public"."command_source_dispositions" ("repository_id", "source_comment_id", "disposition")
  SELECT DISTINCT "repository_id", "source_comment_id", 'batch_created' FROM "public"."batch_command_origins"
  WHERE "command" = 'apply'
  ON CONFLICT DO NOTHING;
