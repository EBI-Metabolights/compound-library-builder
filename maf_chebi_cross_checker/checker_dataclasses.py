from dataclasses import dataclass, field


@dataclass
class IDRegistry:
    total: int = 0
    primary: set = field(default_factory=set)
    secondary: set = field(default_factory=set)
    incorrect: set = field(default_factory=set)


@dataclass
class IDWatchdog:
    maf: IDRegistry
    db: IDRegistry


@dataclass
class OverviewMetrics:
    total_studies: int
    studies_processed: int
    total_mafs: int
    mafs_processed: int