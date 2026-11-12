-- Legacy rows remain unqualified; only new producers opt in at creation.
ALTER TABLE "public"."digger_jobs"
  ADD COLUMN "callback_revision" bigint NOT NULL DEFAULT 0,
  ADD COLUMN "dispatch_protocol_version" bigint NOT NULL DEFAULT 0,
  ADD COLUMN "dependency_retired_at" timestamptz NULL,
  ADD COLUMN "dependency_failure_job_id" text NOT NULL DEFAULT '';

-- Keep retired rows closed even when a previous-version status writer resumes.
CREATE FUNCTION "public"."protect_dependency_retirement"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- Old scalar writers remain compatible only while the entire retained batch
  -- is unqualified. New writers still use their own revision CAS in every mode.
  IF (OLD.dispatch_protocol_version <> 0 OR EXISTS (
      SELECT 1 FROM "public"."digger_jobs" qualified
      WHERE qualified.batch_id = OLD.batch_id AND qualified.dispatch_protocol_version <> 0
     )) AND OLD.callback_revision > 0 AND
    (NEW.callback_revision < OLD.callback_revision OR NEW.callback_revision > OLD.callback_revision + 1 OR
     (NEW.callback_revision = OLD.callback_revision AND
      (NEW.status IS DISTINCT FROM OLD.status OR NEW.status_updated_at IS DISTINCT FROM OLD.status_updated_at OR
       NEW.workflow_run_id IS DISTINCT FROM OLD.workflow_run_id OR NEW.workflow_run_attempt IS DISTINCT FROM OLD.workflow_run_attempt))) THEN
    RAISE EXCEPTION 'job callback revision cannot be bypassed';
  END IF;
  IF OLD.dependency_retired_at IS NOT NULL AND
    (NEW.dependency_retired_at IS DISTINCT FROM OLD.dependency_retired_at OR
     NEW.dependency_failure_job_id IS DISTINCT FROM OLD.dependency_failure_job_id OR
     NEW.status IS DISTINCT FROM 3 OR NEW.workflow_dispatch_started_at IS NOT NULL OR
     COALESCE(NEW.workflow_run_id, 0) <> 0 OR COALESCE(NEW.workflow_run_attempt, 0) <> 0 OR
     NEW.workflow_run_url IS DISTINCT FROM OLD.workflow_run_url OR
     NEW.dispatch_protocol_version IS DISTINCT FROM OLD.dispatch_protocol_version OR
     NEW.digger_job_id IS DISTINCT FROM OLD.digger_job_id OR NEW.batch_id IS DISTINCT FROM OLD.batch_id OR
     NEW.serialized_job_spec IS DISTINCT FROM OLD.serialized_job_spec) THEN
    RAISE EXCEPTION 'dependency-retired job cannot be reopened';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER "protect_dependency_retirement" BEFORE UPDATE ON "public"."digger_jobs"
  FOR EACH ROW EXECUTE FUNCTION "public"."protect_dependency_retirement"();
