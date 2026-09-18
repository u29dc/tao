import unittest

import relevance


class RelevanceTests(unittest.TestCase):
    def test_fixed_relevance_judgments_detect_wrong_rank_missing_sources_and_limit_drift(self):
        expected = {'exact.md': 3, 'support.txt': 2}
        correct = relevance.score(['exact.md', 'support.txt'], expected)
        self.assertTrue(correct['top1'])
        self.assertEqual(correct['recall_at_10'], 1.0)
        self.assertEqual(correct['ndcg_at_10'], 1.0)
        wrong = relevance.score(['support.txt', 'noise.pdf'], expected)
        self.assertFalse(wrong['top1'])
        self.assertEqual(wrong['recall_at_10'], 0.5)
        self.assertLess(wrong['ndcg_at_10'], 1.0)
        self.assertFalse(relevance.score(['unexpected.bin'], {})['empty_correct'])
        self.assertFalse(
            relevance.stable_limits({1: ['wrong.md'], 10: ['exact.md', 'support.txt']})
        )
        with self.assertRaises(ValueError):
            relevance.score(['exact.md', 'exact.md'], expected)
        payload = relevance.sources()
        self.assertEqual(relevance.corpus_hash(payload), relevance.corpus_hash(relevance.sources()))
        self.assertTrue(
            all(set(case['relevance']).issubset(payload) for case in relevance.JUDGMENTS)
        )

    def test_relevance_coverage_requires_complete_exact_corpus_not_missing_zero_fields(self):
        coverage = {
            'total_files': 156,
            'searchable_files': 154,
            'markdown_files': 151,
            'extracted_files': 3,
            'unsupported_files': 2,
            'pending_files': 0,
            'failed_files': 0,
            'partial_files': 0,
            'stale_files': 0,
            'diagnostic_files': 0,
            'complete': True,
        }
        payload = relevance.sources()
        relevance.validate_coverage(coverage, payload)
        text_coverage = {
            **coverage,
            'total_files': 2,
            'searchable_files': 2,
            'markdown_files': 0,
            'extracted_files': 2,
            'unsupported_files': 0,
        }
        relevance.validate_coverage(text_coverage, payload, ['--scope', 'texts'])
        relevance.validate_coverage(text_coverage, payload, ['--ext', 'txt'])
        with self.assertRaises(ValueError):
            relevance.validate_coverage(coverage, payload, ['--scope', 'texts'])
        for invalid in (
            None,
            {},
            {**coverage, 'complete': False},
            {**coverage, 'pending_files': 1},
            {**coverage, 'extracted_files': 2},
            {**coverage, 'failed_files': False},
            {key: value for key, value in coverage.items() if key != 'stale_files'},
        ):
            with self.subTest(coverage=invalid), self.assertRaises(ValueError):
                relevance.validate_coverage(invalid, payload)
