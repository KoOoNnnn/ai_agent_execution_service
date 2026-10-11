-- 1. Check the active database and account.
SELECT current_database(), current_user;

-- 2. Inspect the runs table columns and defaults.
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'runs'
ORDER BY ordinal_position;

-- 3. Preview records.
SELECT id, input_payload, status, cancel_requested, created_at, updated_at
FROM public.runs
ORDER BY created_at DESC
LIMIT 50;

-- Example business stock, including isolated verification SKUs.
SELECT sku, available FROM public.inventory ORDER BY sku;

-- Join each committed effect to its original action and optional successful reservation.
SELECT e.run_id, e.operation_id, a.iteration_index, a.status AS action_status,
       e.result_payload, r.id AS reservation_id, r.sku, r.quantity
FROM public.tool_effects AS e
JOIN public.actions AS a ON a.id = e.operation_id
LEFT JOIN public.reservations AS r ON r.operation_id = e.operation_id
ORDER BY e.created_at DESC
LIMIT 50;

-- Inspect decisions committed before execution; DECIDED does not prove a tool never ran.
SELECT id, run_id, iteration_index, status, decision_payload, created_at
FROM public.actions
ORDER BY created_at DESC
LIMIT 50;

-- 4. See the applied schema revision.
SELECT version_num FROM public.alembic_version;

-- 5. Inspect current execution ownership; terminal runs release their claims.
SELECT id, status, worker_id, claim_id, lease_expires_at, iteration_count
FROM public.runs
ORDER BY created_at DESC
LIMIT 50;

-- Original deadlines and durable per-round decision attempts (including first call).
SELECT r.id, r.status, r.deadline_at, r.decision_max_attempts,
       b.iteration_index, b.attempt_count, r.error_payload
FROM public.runs AS r
LEFT JOIN public.decision_budgets AS b ON b.run_id = r.id
ORDER BY r.created_at DESC, b.iteration_index
LIMIT 50;
