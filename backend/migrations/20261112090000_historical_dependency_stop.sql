CREATE TABLE "public"."digger_dependency_stops" (
  "operation_id" uuid PRIMARY KEY,
  "kind" text NOT NULL DEFAULT 'future_dispatch_fenced' CHECK (kind = 'future_dispatch_fenced'),
  "prior_execution" text NOT NULL DEFAULT 'unknown' CHECK (prior_execution = 'unknown'),
  "organisation_id" bigint NOT NULL REFERENCES "public"."organisations" ("id"),
  "repo_id" bigint NOT NULL REFERENCES "public"."repos" ("id"),
  "batch_id" text NOT NULL REFERENCES "public"."digger_batches" ("id"),
  "target_job_db_id" bigint NOT NULL UNIQUE REFERENCES "public"."digger_jobs" ("id"),
  "request_sha256" varchar(64) NOT NULL,
  "plan_sha256" varchar(64) NOT NULL,
  "request" jsonb NOT NULL,
  "snapshot" jsonb NOT NULL,
  "requested_by" text NOT NULL,
  "fenced_at" timestamptz NOT NULL,
  "verified_at" timestamptz NULL,
  "verified_by" text NOT NULL DEFAULT '',
  "verification" jsonb NULL,
  CHECK ((verified_at IS NULL AND verified_by = '' AND verification IS NULL) OR
         (verified_at >= fenced_at AND verified_by <> '' AND jsonb_typeof(verification) = 'object'))
);

ALTER TABLE "public"."digger_jobs"
  ADD COLUMN "dependency_retirement_operation_id" uuid NULL
  REFERENCES "public"."digger_dependency_stops" ("operation_id");

CREATE FUNCTION "public"."protect_dependency_stop_audit"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'historical dependency stop audit cannot be deleted';
  END IF;
  IF (NEW.operation_id, NEW.kind, NEW.prior_execution, NEW.organisation_id, NEW.repo_id, NEW.batch_id, NEW.target_job_db_id,
      NEW.request_sha256, NEW.plan_sha256, NEW.request, NEW.snapshot, NEW.requested_by, NEW.fenced_at)
      IS DISTINCT FROM
     (OLD.operation_id, OLD.kind, OLD.prior_execution, OLD.organisation_id, OLD.repo_id, OLD.batch_id, OLD.target_job_db_id,
      OLD.request_sha256, OLD.plan_sha256, OLD.request, OLD.snapshot, OLD.requested_by, OLD.fenced_at) THEN
    RAISE EXCEPTION 'historical dependency stop identity is immutable';
  END IF;
  IF OLD.verified_at IS NOT NULL AND
     (NEW.verified_at, NEW.verified_by, NEW.verification) IS DISTINCT FROM
     (OLD.verified_at, OLD.verified_by, OLD.verification) THEN
    RAISE EXCEPTION 'historical dependency stop verification is immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER "protect_dependency_stop_audit" BEFORE UPDATE OR DELETE ON "public"."digger_dependency_stops"
  FOR EACH ROW EXECUTE FUNCTION "public"."protect_dependency_stop_audit"();

CREATE FUNCTION "public"."protect_dependency_stop_job"() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  stop_record "public"."digger_dependency_stops"%ROWTYPE;
BEGIN
  IF OLD.dependency_retirement_operation_id IS NOT NULL AND
     NEW.dependency_retirement_operation_id IS DISTINCT FROM OLD.dependency_retirement_operation_id THEN
    RAISE EXCEPTION 'historical dependency stop cannot be detached';
  END IF;
  IF NEW.dependency_retirement_operation_id IS NOT NULL THEN
    SELECT * INTO stop_record FROM "public"."digger_dependency_stops"
      WHERE operation_id = NEW.dependency_retirement_operation_id;
    IF NOT FOUND OR stop_record.target_job_db_id <> NEW.id OR stop_record.batch_id::text <> NEW.batch_id OR
       stop_record.request->'targets'->0->>'job_id' IS DISTINCT FROM NEW.digger_job_id OR
       stop_record.request->'failed_prerequisite'->>'job_id' IS DISTINCT FROM NEW.dependency_failure_job_id OR
       stop_record.fenced_at IS DISTINCT FROM NEW.dependency_retired_at OR
       NEW.dispatch_protocol_version <> 0 OR NEW.status <> 3 OR NEW.workflow_dispatch_started_at IS NOT NULL OR
       COALESCE(NEW.workflow_run_id,0) <> 0 OR COALESCE(NEW.workflow_run_attempt,0) <> 0 OR
       COALESCE(NEW.workflow_run_url,'') NOT IN ('','#') THEN
      RAISE EXCEPTION 'historical dependency stop must preserve unknown prior execution';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER "protect_dependency_stop_job" BEFORE UPDATE ON "public"."digger_jobs"
  FOR EACH ROW EXECUTE FUNCTION "public"."protect_dependency_stop_job"();

-- The previous producer reads numeric job failure as terminal. Keep it from
-- closing this batch before the separately verified historical policy checkpoint.
CREATE FUNCTION "public"."protect_pending_dependency_stop_batch"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status IN (3,4,5) AND EXISTS (
      SELECT 1 FROM "public"."digger_dependency_stops" s
      WHERE s.batch_id = NEW.id AND s.verified_at IS NULL
     ) THEN
    RAISE EXCEPTION 'historical dependency stop awaits verification';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER "protect_pending_dependency_stop_batch" BEFORE UPDATE OF "status" ON "public"."digger_batches"
  FOR EACH ROW EXECUTE FUNCTION "public"."protect_pending_dependency_stop_batch"();

CREATE TABLE public.digger_dependency_stop_checkpoints (
 id uuid PRIMARY KEY,
 operation_id uuid NOT NULL REFERENCES public.digger_dependency_stops(operation_id),
 input_sha256 varchar(64) NOT NULL,
 local_state_sha256 varchar(64) NOT NULL,
 job_states jsonb NOT NULL CHECK(jsonb_typeof(job_states)='array'),
 verification jsonb NOT NULL CHECK(jsonb_typeof(verification)='object'),
 verified_at timestamptz NOT NULL,
 verified_by text NOT NULL
);
CREATE UNIQUE INDEX idx_dependency_checkpoint_input ON public.digger_dependency_stop_checkpoints(operation_id,input_sha256);
CREATE INDEX idx_digger_dependency_stop_checkpoints_local_state_sha256 ON public.digger_dependency_stop_checkpoints(local_state_sha256);
CREATE FUNCTION public.protect_dependency_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'historical settlement checkpoints are append-only'; END;
$$;
CREATE TRIGGER protect_dependency_checkpoint BEFORE UPDATE OR DELETE ON public.digger_dependency_stop_checkpoints FOR EACH ROW EXECUTE FUNCTION public.protect_dependency_checkpoint();

-- This predicate compares exactly the execution-state fields hashed by
-- dependencyStopStates. Timestamps are instants; the aggregate is derived.
CREATE FUNCTION public.dependency_stop_has_current_checkpoint(stop_id uuid) RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS (
  SELECT 1 FROM public.digger_dependency_stops s
  JOIN public.digger_batches b ON b.id=s.batch_id
  JOIN public.digger_dependency_stop_checkpoints c ON c.operation_id=s.operation_id
  WHERE s.operation_id=stop_id
   AND b.comment_id=(s.snapshot->>'report_comment_id')::bigint
   AND b.check_run_id=s.snapshot->>'aggregate_check_id'
   AND b.repo_full_name=s.snapshot->'origin'->>'repo_full_name'
   AND b.commit_sha=s.snapshot->'origin'->>'commit_sha'
   AND b.pr_number=(s.snapshot->'origin'->>'pr_number')::bigint
   AND b.github_installation_id=(s.snapshot->>'installation_id')::bigint
   AND b.vcs='github' AND b.batch_type=s.snapshot->'origin'->>'command'
   AND b.repo_owner || '/' || b.repo_name = s.snapshot->'origin'->>'repo_full_name'
   AND EXISTS(SELECT 1 FROM public.batch_command_origins o WHERE o.batch_id::text=s.batch_id AND o.sealed=true AND o.job_manifest::jsonb=s.snapshot->'manifest' AND o.body_sha256=s.snapshot->'origin'->>'body_sha256' AND o.source_comment_id=(s.snapshot->'origin'->>'source_comment_id')::bigint AND o.repository_id=(s.snapshot->'origin'->>'repository_id')::bigint AND o.github_installation_id=b.github_installation_id AND o.commit_sha=b.commit_sha AND o.repo_full_name=b.repo_full_name AND o.pr_number=b.pr_number AND o.command=s.snapshot->'origin'->>'command' AND o.aggregate_check_id=b.check_run_id)
   AND COALESCE((SELECT jsonb_agg(jsonb_build_object('id',l.id,'job_id',l.digger_job_id,'parent_id',l.parent_digger_job_id) ORDER BY l.id) FROM public.digger_job_parent_links l WHERE l.deleted_at IS NULL AND (l.digger_job_id IN (SELECT digger_job_id FROM public.digger_jobs WHERE batch_id=s.batch_id) OR l.parent_digger_job_id IN(SELECT digger_job_id FROM public.digger_jobs WHERE batch_id=s.batch_id))),'[]'::jsonb)=s.snapshot->'edges'
   AND COALESCE((SELECT jsonb_agg(jsonb_build_object('id',l.id,'job_id',l.digger_job_id,'repo',l.repo_full_name,'workflow_run_id',l.github_workflow_run_id,'github_job_id',l.github_job_id,'deleted',l.deleted_at IS NOT NULL) ORDER BY l.id) FROM public.github_digger_job_links l WHERE l.digger_job_id=s.request->'targets'->0->>'job_id'),'[]'::jsonb)=s.snapshot->'links'
   AND jsonb_array_length(c.job_states)=(SELECT count(*) FROM public.digger_jobs j WHERE j.batch_id=s.batch_id)
   AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(c.job_states) v
    LEFT JOIN public.digger_jobs j ON j.id=(v->>'db_id')::bigint AND j.batch_id=s.batch_id
    WHERE j.id IS NULL OR j.deleted_at IS NOT NULL OR j.digger_job_id IS DISTINCT FROM v->>'job_id'
     OR j.project_name IS DISTINCT FROM v->>'project' OR COALESCE(j.check_run_id,'') IS DISTINCT FROM v->>'project_check_id'
     OR COALESCE(j.status,0) IS DISTINCT FROM (v->>'status')::int
     OR COALESCE(j.callback_revision,0) IS DISTINCT FROM (v->>'callback_revision')::bigint
     OR COALESCE(j.status_updated_at,'0001-01-01 00:00:00+00'::timestamptz) IS DISTINCT FROM (v->>'status_updated_at')::timestamptz
     OR j.workflow_dispatch_started_at IS DISTINCT FROM (v->>'dispatch_claim')::timestamptz
     OR COALESCE(j.workflow_run_id,0) IS DISTINCT FROM (v->>'workflow_run_id')::bigint
     OR COALESCE(j.workflow_run_attempt,0) IS DISTINCT FROM (v->>'workflow_run_attempt')::bigint
     OR COALESCE(j.workflow_run_url,'') IS DISTINCT FROM v->>'workflow_url'
     OR COALESCE(j.dispatch_protocol_version,0) IS DISTINCT FROM (v->>'dispatch_protocol_version')::int
     OR encode(sha256(COALESCE(j.serialized_job_spec,''::bytea)),'hex') IS DISTINCT FROM v->>'spec_sha256'
     OR j.dependency_retired_at IS DISTINCT FROM (v->>'retired_at')::timestamptz
     OR j.dependency_retirement_operation_id IS DISTINCT FROM (v->>'stop_id')::uuid
   )
 );
$$;
CREATE OR REPLACE FUNCTION public.protect_pending_dependency_stop_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.status IN (3,4,5) AND EXISTS(SELECT 1 FROM public.digger_dependency_stops s WHERE s.batch_id=NEW.id) AND (EXISTS(SELECT 1 FROM public.digger_dependency_stops s WHERE s.batch_id=NEW.id AND NOT public.dependency_stop_has_current_checkpoint(s.operation_id)) OR EXISTS(SELECT 1 FROM public.digger_jobs j WHERE j.batch_id=NEW.id AND (j.deleted_at IS NOT NULL OR j.status IS NULL OR j.status NOT IN (3,5)))) THEN
  RAISE EXCEPTION 'historical dependency stop awaits current settlement verification';
 END IF;
 RETURN NEW;
END;
$$;
CREATE FUNCTION public.invalidate_dependency_checkpoint_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM public.digger_dependency_stops s WHERE s.batch_id=NEW.batch_id AND NOT public.dependency_stop_has_current_checkpoint(s.operation_id)) THEN
  UPDATE public.digger_batches SET status=2 WHERE id=NEW.batch_id AND status IN (3,5);
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER invalidate_dependency_checkpoint_batch AFTER UPDATE ON public.digger_jobs FOR EACH ROW EXECUTE FUNCTION public.invalidate_dependency_checkpoint_batch();

CREATE FUNCTION public.protect_historical_callback_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.dependency_retirement_operation_id IS NULL AND EXISTS(SELECT 1 FROM public.digger_dependency_stops s WHERE s.batch_id=OLD.batch_id) AND
 (NEW.callback_revision<OLD.callback_revision OR NEW.callback_revision>OLD.callback_revision+1 OR
  (NEW.callback_revision=OLD.callback_revision AND (NEW.status IS DISTINCT FROM OLD.status OR NEW.status_updated_at IS DISTINCT FROM OLD.status_updated_at OR NEW.workflow_run_id IS DISTINCT FROM OLD.workflow_run_id OR NEW.workflow_run_attempt IS DISTINCT FROM OLD.workflow_run_attempt) AND
   NOT (OLD.workflow_dispatch_started_at IS NULL AND NEW.workflow_dispatch_started_at IS NOT NULL AND COALESCE(OLD.workflow_run_id,0)=0 AND COALESCE(NEW.workflow_run_id,0)=0 AND NEW.status=2))) THEN
  RAISE EXCEPTION 'historical batch callback revision cannot be bypassed';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER protect_historical_callback_revision BEFORE UPDATE ON public.digger_jobs FOR EACH ROW EXECUTE FUNCTION public.protect_historical_callback_revision();
