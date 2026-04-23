# SaaS Transformation Oath

We are transforming TrashPanda from an advanced internal tool into a SaaS
product.

We will protect the engine.

We will not casually rewrite the pipeline, scoring, validation, deduplication,
typo suggestion, reporting, review, or export logic that already creates value.

We will not confuse local infrastructure with SaaS infrastructure.

`InMemoryJobStore`, FastAPI background tasks, local runtime directories, mock
adapters, preview pages, and global job access are temporary. They are not the
platform.

We will keep the Engine and Platform separate.

The Engine processes data. The Platform authenticates users, scopes
organizations, persists jobs, authorizes access, stores artifacts, records
audit events, and operates the product.

We will not put users, organizations, billing, sessions, or roles inside engine
logic.

We will not duplicate engine decisions in the frontend or platform layer.

We will preserve the critical flows:

- Upload.
- Job creation.
- Processing.
- Results.
- Logs or processing events.
- Review queue.
- Review decisions.
- Exports.
- Insights.

We will not ship global job access as SaaS.

We will not expose customer data without authentication, authorization, and
organization scope.

We will not let public request input choose server-side configuration.

We will not represent mock data, preview pages, or disabled modules as completed
product capabilities.

We will keep product claims honest.

TrashPanda reduces data hygiene risk. It does not guarantee inbox existence,
perfect deliverability, or zero bounces.

We will evolve the system by wrapping, securing, persisting, and operating the
working engine, not by erasing it.
