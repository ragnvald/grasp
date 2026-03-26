from __future__ import annotations

from grasp.intelligence.providers import (
    CandidateRanker,
    DuckDuckGoSearchProvider,
    HeuristicClassificationProvider,
    HeuristicSearchProvider,
    OpenAIClassificationProvider,
    SourceSearchProvider,
)
from grasp.models import DatasetRecord, DatasetUnderstanding, SourceCandidate


class IntelligenceService:
    def __init__(self, classifier=None) -> None:
        self.classifier = classifier or OpenAIClassificationProvider(fallback=HeuristicClassificationProvider())

    def classify(self, dataset: DatasetRecord) -> DatasetUnderstanding:
        return self.classifier.classify(dataset)

    def enrich_from_sources(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        candidates: list[SourceCandidate],
    ) -> DatasetUnderstanding:
        enricher = getattr(self.classifier, "enrich_from_sources", None)
        if callable(enricher):
            return enricher(dataset, understanding, candidates)
        return understanding

    def group_datasets(
        self,
        datasets: list[DatasetRecord],
        target_group_count: int,
        *,
        timeout_s: float | None = None,
        group_count_bounds: tuple[int, int] | None = None,
    ) -> dict[str, str]:
        grouper = getattr(self.classifier, "group_datasets", None)
        if callable(grouper):
            try:
                return grouper(
                    datasets,
                    target_group_count,
                    timeout_s=timeout_s,
                    group_count_bounds=group_count_bounds,
                )
            except TypeError:
                return grouper(datasets, target_group_count, timeout_s=timeout_s)
        return {}


class SearchService:
    def __init__(
        self,
        *,
        provider: SourceSearchProvider | None = None,
        ranker: CandidateRanker | None = None,
    ) -> None:
        self.provider = provider or DuckDuckGoSearchProvider()
        self.fallback_provider = HeuristicSearchProvider()
        self.ranker = ranker or OpenAIClassificationProvider(fallback=HeuristicClassificationProvider())

    def find_sources(
        self,
        understanding: DatasetUnderstanding,
        dataset: DatasetRecord | None = None,
    ) -> list[SourceCandidate]:
        queries = understanding.search_queries[:3]
        candidates = self.provider.search(queries)
        if not candidates:
            candidates = self.fallback_provider.search(queries)
        if dataset is None:
            dataset = DatasetRecord(
                dataset_id="",
                source_path="",
                source_format="",
                display_name_ai=understanding.suggested_title,
                geometry_type="",
                feature_count=0,
            )
        ranked = self.ranker.rank(dataset, understanding, candidates)
        return ranked[:5]
