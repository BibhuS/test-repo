from databricks.bundles.core import (
    Bundle,
    ResourceMutator,
)
from databricks.bundles.jobs import Job


class ScheduleMutator(ResourceMutator):
    def __init__(self):
        super().__init__()

    def mutate(self, bundle: Bundle) -> None:
        environment = bundle.target

        # Get environment-specific default pause status
        default_pause_status = self._get_default_pause_status(
            bundle,
            environment,
        )

        # Get environment-specific overrides
        overrides = self._get_schedule_overrides(
            bundle,
            environment,
        )

        # Process every job in the bundle
        for job_name, job in bundle.resources.jobs.items():

            # Start with environment default
            pause_status = default_pause_status
            cron_expression = None

            # Check whether this job has an override
            override = overrides.get(job_name)

            if override:
                if "pause_status" in override:
                    pause_status = override["pause_status"]

                if "cron" in override:
                    cron_expression = override["cron"]

            # Apply scheduling configuration
            self._apply_schedule(
                job,
                pause_status,
                cron_expression,
            )

    def _get_default_pause_status(
        self,
        bundle: Bundle,
        environment: str,
    ) -> str:

        if environment == "dev":
            return bundle.variables["dev_schedule_pause_status"]

        if environment == "test":
            return bundle.variables["test_schedule_pause_status"]

        # PROD keeps existing behaviour
        return "UNPAUSED"

    def _get_schedule_overrides(
        self,
        bundle: Bundle,
        environment: str,
    ) -> dict:

        schedule_overrides = bundle.variables.get(
            "schedule_overrides",
            {},
        )

        return schedule_overrides.get(
            environment,
            {},
        )

    def _apply_schedule(
        self,
        job: Job,
        pause_status: str,
        cron_expression: str | None,
    ) -> None:

        if job.schedule is None:
            return

        # Always apply pause/unpause
        job.schedule.pause_status = pause_status

        # Only replace cron when an override exists
        if cron_expression:
            job.schedule.quartz_cron_expression = cron_expression


def load() -> list[ResourceMutator]:
    return [
        ScheduleMutator(),
    ]
