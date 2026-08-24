"""Run the real create-cluster retry loop, old and new, against a fake Dataproc client."""
import uuid
from google.api_core.exceptions import NotFound, InvalidArgument

MAX_CREATE_RETRIES, RETRY_WAIT_SECONDS = 3, 0
QUOTA = "Multiple validation errors:\n - Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0."


class Log:
    def __init__(self): self.lines = []
    def info(self, m): self.lines.append(("info", m))
    def error(self, m): self.lines.append(("error", m))
    def warning(self, m): self.lines.append(("warn", m))


class FakeClient:
    """create_cluster refuses for `refuse_first` attempts, then succeeds. Nothing to delete."""
    def __init__(self, refuse_first): self.refuse_first, self.creates, self.deletes = refuse_first, 0, 0
    def create_cluster(self, request):
        self.creates += 1
        if self.creates <= self.refuse_first:
            raise InvalidArgument(QUOTA)
        class Op:
            def result(self):
                class R:
                    class config:
                        class endpoint_config: http_ports = {}
                return R()
        return Op()
    def delete_cluster(self, request):
        self.deletes += 1
        raise NotFound(f"404 Not found: Cluster .../clusters/{request['cluster_name']}")
    def get_cluster(self, request): raise NotFound("no cluster")


def run(client, logger, guarded, chain):
    def _delete_cluster_before_retry(c, p, r, n):
        logger.info(f"Deleting cluster {n} to free quota before retry...")
        if guarded:
            try:
                c.delete_cluster(request={"project_id": p, "region": r, "cluster_name": n}).result()
                logger.info(f"Cluster {n} deleted successfully")
            except Exception as cleanup_error:
                logger.warning(f"Could not delete cluster {n}: {cleanup_error}")
        else:
            c.delete_cluster(request={"project_id": p, "region": r, "cluster_name": n}).result()
            logger.info(f"Cluster {n} deleted successfully")

    def _cluster_in_error_state(c, p, r, n):
        try:
            return c.get_cluster(request={"project_id": p, "region": r, "cluster_name": n}).status.state.name == "ERROR"
        except Exception:
            return False

    cluster_name, result = "fangorn-challenger-seed", None
    for attempt in range(1, MAX_CREATE_RETRIES + 1):
        try:
            if attempt > 1:
                cluster_name = f"fangorn-challenger-{uuid.uuid4().hex[:8]}"
            operation = client.create_cluster(request={"cluster": {"cluster_name": cluster_name}})
            result = operation.result()
            if _cluster_in_error_state(client, "p", "r", cluster_name):
                logger.error(f"Cluster {cluster_name} entered ERROR state after creation")
                _delete_cluster_before_retry(client, "p", "r", cluster_name)
                if attempt < MAX_CREATE_RETRIES:
                    continue
                raise RuntimeError("Cluster creation failed after 3 attempts — cluster kept entering ERROR state")
            logger.info(f"Cluster {cluster_name} created successfully")
            break
        except Exception as e:
            if result is not None and "ERROR state" in str(e):
                raise
            logger.error(f"Cluster creation attempt {attempt}/{MAX_CREATE_RETRIES} failed: {e}")
            _delete_cluster_before_retry(client, "p", "r", cluster_name)
            if attempt < MAX_CREATE_RETRIES:
                pass
            elif chain:
                raise RuntimeError(f"Cluster creation failed after {MAX_CREATE_RETRIES} attempts: {e}") from e
            else:
                raise RuntimeError(f"Cluster creation failed after {MAX_CREATE_RETRIES} attempts: {e}")
    return result


def case(name, refuse_first, guarded):
    c, lg = FakeClient(refuse_first), Log()
    try:
        run(c, lg, guarded, chain=guarded)
        outcome = "SUCCEEDED"
    except Exception as e:
        outcome = f"{type(e).__name__}: {str(e)[:70]}"
    print(f"{name:42} creates={c.creates}  outcome={outcome}")


print("== healthy run: create succeeds first try ==")
case("OLD (main)", 0, False)
case("NEW (PR #93)", 0, True)
print("\n== quota refuses twice, then frees (INC-025's shape) ==")
case("OLD (main)", 2, False)
case("NEW (PR #93)", 2, True)
print("\n== quota never frees ==")
case("OLD (main)", 99, False)
case("NEW (PR #93)", 99, True)
