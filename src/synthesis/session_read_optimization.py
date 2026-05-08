"""Session read optimization analyzer for workflow reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class ReadOperation:
    turn_index: int
    file_path: str
    offset: int | None
    limit: int | None
    bytes_read: int


@dataclass(frozen=True)
class EditOperation:
    turn_index: int
    file_path: str
    edit_size_bytes: int


@dataclass(frozen=True)
class ReadOptimizationMetrics:
    total_reads: int
    targeted_reads: int
    full_reads: int
    repeated_reads: int
    wasteful_reads: int  # full reads after small edits
    targeted_read_percentage: float
    repeated_read_rate: float
    wasteful_read_rate: float
    average_bytes_per_read: float
    bytes_read_per_edit: float


@dataclass(frozen=True)
class ReadOptimizationExample:
    turn_index: int
    file_path: str
    read_type: str  # "targeted", "full", "repeated", "wasteful"
    bytes_read: int
    previous_edit_size: int | None


@dataclass(frozen=True)
class SessionReadOptimization:
    metrics: ReadOptimizationMetrics
    examples: tuple[ReadOptimizationExample, ...]
    insights: tuple[str, ...]


SMALL_EDIT_THRESHOLD = 500  # bytes


def analyze_session_read_optimization(
    reads: Sequence[ReadOperation],
    edits: Sequence[EditOperation],
) -> SessionReadOptimization:
    """Measure file read efficiency and detect wasteful read patterns."""
    _validate_read_operations(reads)
    _validate_edit_operations(edits)

    if not reads:
        metrics = ReadOptimizationMetrics(
            total_reads=0,
            targeted_reads=0,
            full_reads=0,
            repeated_reads=0,
            wasteful_reads=0,
            targeted_read_percentage=0.0,
            repeated_read_rate=0.0,
            wasteful_read_rate=0.0,
            average_bytes_per_read=0.0,
            bytes_read_per_edit=0.0,
        )
        return SessionReadOptimization(
            metrics=metrics,
            examples=(),
            insights=("No read operations detected.",),
        )

    # Track file reads and edits
    reads_by_file: dict[str, list[ReadOperation]] = {}
    edits_by_file: dict[str, list[EditOperation]] = {}

    for read in reads:
        if read.file_path not in reads_by_file:
            reads_by_file[read.file_path] = []
        reads_by_file[read.file_path].append(read)

    for edit in edits:
        if edit.file_path not in edits_by_file:
            edits_by_file[edit.file_path] = []
        edits_by_file[edit.file_path].append(edit)

    targeted_reads = 0
    full_reads = 0
    repeated_reads = 0
    wasteful_reads = 0
    total_bytes = 0
    examples: list[ReadOptimizationExample] = []

    for file_path, file_reads in reads_by_file.items():
        file_reads_sorted = sorted(file_reads, key=lambda r: r.turn_index)
        file_edits = edits_by_file.get(file_path, [])
        file_edits_sorted = sorted(file_edits, key=lambda e: e.turn_index)

        prev_read: ReadOperation | None = None

        for read in file_reads_sorted:
            is_targeted = read.offset is not None or read.limit is not None
            total_bytes += read.bytes_read

            if is_targeted:
                targeted_reads += 1
            else:
                full_reads += 1

            # Check if repeated (reading same file multiple times)
            if prev_read is not None:
                repeated_reads += 1
                if len(examples) < 5:
                    examples.append(
                        ReadOptimizationExample(
                            turn_index=read.turn_index,
                            file_path=file_path,
                            read_type="repeated",
                            bytes_read=read.bytes_read,
                            previous_edit_size=None,
                        )
                    )

            # Check if wasteful (full read after small edit)
            if not is_targeted:
                # Find most recent edit before this read
                recent_edit = _find_recent_edit(read, file_edits_sorted)
                if recent_edit and recent_edit.edit_size_bytes < SMALL_EDIT_THRESHOLD:
                    wasteful_reads += 1
                    if len(examples) < 5:
                        examples.append(
                            ReadOptimizationExample(
                                turn_index=read.turn_index,
                                file_path=file_path,
                                read_type="wasteful",
                                bytes_read=read.bytes_read,
                                previous_edit_size=recent_edit.edit_size_bytes,
                            )
                        )

            prev_read = read

    targeted_read_percentage = _percentage(targeted_reads, len(reads))
    repeated_read_rate = _percentage(repeated_reads, len(reads))
    wasteful_read_rate = _percentage(wasteful_reads, len(reads))
    average_bytes_per_read = total_bytes / len(reads) if reads else 0.0
    bytes_read_per_edit = total_bytes / len(edits) if edits else 0.0

    metrics = ReadOptimizationMetrics(
        total_reads=len(reads),
        targeted_reads=targeted_reads,
        full_reads=full_reads,
        repeated_reads=repeated_reads,
        wasteful_reads=wasteful_reads,
        targeted_read_percentage=targeted_read_percentage,
        repeated_read_rate=repeated_read_rate,
        wasteful_read_rate=wasteful_read_rate,
        average_bytes_per_read=round(average_bytes_per_read, 2),
        bytes_read_per_edit=round(bytes_read_per_edit, 2),
    )

    return SessionReadOptimization(
        metrics=metrics,
        examples=tuple(examples),
        insights=_generate_insights(metrics),
    )


def _validate_read_operations(reads: Sequence[ReadOperation]) -> None:
    """Validate read operation structure."""
    if not isinstance(reads, (list, tuple)):
        raise ValueError("reads must be a list or tuple")

    last_turn = -1
    for read in reads:
        if not isinstance(read, ReadOperation):
            raise ValueError("reads must contain ReadOperation instances")
        if not isinstance(read.turn_index, int) or isinstance(read.turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if read.turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if not isinstance(read.file_path, str):
            raise ValueError("file_path must be a string")
        if not read.file_path.strip():
            raise ValueError("file_path must not be empty")
        if read.offset is not None and not isinstance(read.offset, int):
            raise ValueError("offset must be an integer or None")
        if read.limit is not None and not isinstance(read.limit, int):
            raise ValueError("limit must be an integer or None")
        if not isinstance(read.bytes_read, int):
            raise ValueError("bytes_read must be an integer")
        if read.bytes_read < 0:
            raise ValueError("bytes_read must be non-negative")

        if read.turn_index <= last_turn:
            raise ValueError("reads must have strictly increasing turn_index")

        last_turn = read.turn_index


def _validate_edit_operations(edits: Sequence[EditOperation]) -> None:
    """Validate edit operation structure."""
    if not isinstance(edits, (list, tuple)):
        raise ValueError("edits must be a list or tuple")

    last_turn = -1
    for edit in edits:
        if not isinstance(edit, EditOperation):
            raise ValueError("edits must contain EditOperation instances")
        if not isinstance(edit.turn_index, int) or isinstance(edit.turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if edit.turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if not isinstance(edit.file_path, str):
            raise ValueError("file_path must be a string")
        if not edit.file_path.strip():
            raise ValueError("file_path must not be empty")
        if not isinstance(edit.edit_size_bytes, int):
            raise ValueError("edit_size_bytes must be an integer")
        if edit.edit_size_bytes < 0:
            raise ValueError("edit_size_bytes must be non-negative")

        if edit.turn_index <= last_turn:
            raise ValueError("edits must have strictly increasing turn_index")

        last_turn = edit.turn_index


def _find_recent_edit(
    read: ReadOperation,
    edits_sorted: list[EditOperation],
) -> EditOperation | None:
    """Find the most recent edit before the given read."""
    recent_edit: EditOperation | None = None
    for edit in edits_sorted:
        if edit.turn_index < read.turn_index:
            recent_edit = edit
        else:
            break
    return recent_edit


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _generate_insights(metrics: ReadOptimizationMetrics) -> tuple[str, ...]:
    """Generate human-readable insights from metrics."""
    if metrics.total_reads == 0:
        return ("No read operations detected.",)

    insights = []

    insights.append(
        f"{metrics.targeted_reads} of {metrics.total_reads} reads use offset/limit "
        f"({metrics.targeted_read_percentage}% targeted)."
    )

    if metrics.targeted_read_percentage < 50.0 and metrics.total_reads >= 5:
        insights.append(
            "Low targeted read adoption: most reads are full file reads."
        )

    if metrics.repeated_reads > 0:
        insights.append(
            f"{metrics.repeated_reads} repeated reads of same files detected "
            f"({metrics.repeated_read_rate}% repeat rate)."
        )

    if metrics.wasteful_reads > 0:
        insights.append(
            f"{metrics.wasteful_reads} full reads immediately after small edits "
            f"({metrics.wasteful_read_rate}% wasteful rate)."
        )

    if metrics.average_bytes_per_read > 10000:
        insights.append(
            f"High average read size ({metrics.average_bytes_per_read:.0f} bytes): "
            f"consider using targeted reads for large files."
        )

    # Efficiency score
    if (
        metrics.targeted_read_percentage >= 85
        and metrics.wasteful_read_rate < 10
        and metrics.repeated_read_rate < 20
    ):
        insights.append(
            "Excellent read efficiency: high targeted read adoption and low waste."
        )

    return tuple(insights)
