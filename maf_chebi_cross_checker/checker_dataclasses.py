from dataclasses import dataclass, field


@dataclass
class IDRegistry:
    """
    Dataclass to hold sets of compound IDs - primary means it is a 'primary' ChEBI ID, while a 'secondary' means that
    the ChEBI ID it is associated with is a duplicate of another, primary ChEBI ID. Incorrect means an ID that has no
    corresponding entry in ChEBI and so is incorrect.
    """
    total: int = 0
    primary: set = field(default_factory=set)
    secondary: set = field(default_factory=set)
    incorrect: set = field(default_factory=set)


@dataclass
class IDWatchdog:
    """
    Dataclass with two IDRegistries, one, maf, for all IDs found in MetaboLights MAF sheets, and the other, db, for all
    IDs found in the MetaboLights DB compound table.
    """
    maf: IDRegistry
    db: IDRegistry


@dataclass
class OverviewMetrics:
    """
    Simple dataclass used as a running overview of the MAF sheet - ChEBI cross referencing process
    """
    total_studies: int
    studies_processed: int
    total_mafs: int
    mafs_processed: int