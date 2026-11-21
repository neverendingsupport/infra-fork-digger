CREATE TABLE public.command_decisions (
    repository_id bigint NOT NULL CHECK (repository_id > 0),
    source_comment_id bigint NOT NULL CHECK (source_comment_id > 0),
    PRIMARY KEY (repository_id, source_comment_id),
    version integer NOT NULL DEFAULT 0,
    organisation_id bigint,
    repo_id bigint,
    repo_full_name text,
    pr_number bigint,
    body_sha256 text,
    commit_sha text,
    app_id bigint,
    installation_id bigint,
    CHECK (version = 0 OR (version = 1 AND
        organisation_id IS NOT NULL AND repo_id IS NOT NULL AND
        repo_full_name IS NOT NULL AND pr_number IS NOT NULL AND pr_number > 0 AND
        body_sha256 IS NOT NULL AND body_sha256 ~ '^[a-f0-9]{64}$' AND
        commit_sha IS NOT NULL AND commit_sha ~ '^[a-f0-9]{40}$' AND
        app_id IS NOT NULL AND app_id > 0 AND installation_id IS NOT NULL AND installation_id > 0))
);

CREATE FUNCTION public.keep_command_decision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% rows are permanent', TG_TABLE_NAME;
END;
$$;
CREATE TRIGGER keep_command_decision BEFORE UPDATE OR DELETE ON public.command_decisions
    FOR EACH ROW EXECUTE FUNCTION public.keep_command_decision();
-- Row triggers don't fire on TRUNCATE, which would otherwise erase every decision.
CREATE TRIGGER keep_command_decisions_no_truncate BEFORE TRUNCATE ON public.command_decisions
    FOR EACH STATEMENT EXECUTE FUNCTION public.keep_command_decision();

-- .nes.13 must insert an origin before its first job, even on forced applies.
CREATE FUNCTION public.admit_command_origin() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.command_decisions (repository_id, source_comment_id)
        VALUES (NEW.repository_id, NEW.source_comment_id) ON CONFLICT DO NOTHING;
    IF NOT EXISTS (SELECT FROM public.command_decisions
        WHERE repository_id = NEW.repository_id AND source_comment_id = NEW.source_comment_id AND version = 0) THEN
        RAISE EXCEPTION 'source comment permanently rejected before dispatch';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER admit_command_origin BEFORE INSERT ON public.batch_command_origins
    FOR EACH ROW EXECUTE FUNCTION public.admit_command_origin();

CREATE INDEX idx_origin_repository_comment ON public.batch_command_origins(repository_id, source_comment_id);
CREATE INDEX idx_command_rejection_read ON public.command_decisions(repo_id, pr_number, source_comment_id) WHERE version = 1;

CREATE TABLE public.command_rejection_epoch (
    id integer PRIMARY KEY CHECK (id = 1),
    installed_at timestamptz NOT NULL
);
INSERT INTO public.command_rejection_epoch VALUES (1, clock_timestamp());
-- Moving the epoch back would let comments from before the migration qualify.
CREATE TRIGGER keep_command_rejection_epoch BEFORE UPDATE OR DELETE ON public.command_rejection_epoch
    FOR EACH ROW EXECUTE FUNCTION public.keep_command_decision();
CREATE TRIGGER keep_command_rejection_epoch_no_truncate BEFORE TRUNCATE ON public.command_rejection_epoch
    FOR EACH STATEMENT EXECUTE FUNCTION public.keep_command_decision();
