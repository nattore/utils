from ..utils import AwsConfig, Status, State
from typing import Dict, Tuple, Any
import logging

logger = logging.getLogger(__name__)


def _check_primary_datasets(table_statuses: Dict[str, Status]) -> Status:
    """Check if primary datasets were built successfully."""
    check_status = Status(State.OK)
    has_failed = []
    for table, build in table_statuses.items():
        if build.state is State.FAIL:
            logger.error(f"Table {table} failed to be built.")
            has_failed.append(table)
    if has_failed:
        check_status.state = State.FAILED
    return check_status


def build_abt(config: AwsConfig, **table_statuses) -> Tuple[Dict[str, Status], Any]:
    """Construct the Analytical Base Table from primary datasets."""
    _build_abt = State.SUCCESS
    logger.info("Checking states of primary datasets.")
    datasets_state = _check_primary_datasets(table_statuses).state
    if datasets_state is not State.OK:
        logger.error("Some primary datasets failed to be built.")
        _build_abt = State.FAIL
        return {config.table: _build_abt}, None
