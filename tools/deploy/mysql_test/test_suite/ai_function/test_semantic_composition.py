"""Independent relational composition contracts for AI_MAP and AI_FILTER."""

from collections import Counter
from decimal import Decimal
import sys

import test_semantic_runtime as semantic
from test_semantic_runtime import Reply, completion


class SemanticCompositionContracts(semantic.SemanticContracts):
    def test_filter_map_group_having_sort_limit(self):
        prompts = [f"group-{index}" for index in range(67)]
        self.load(prompts)
        counts = Counter()
        accepted = []
        for index, prompt in enumerate(prompts):
            keep = index % 3 != 0
            category = ("alpha", "beta", "gamma")[index % 4 % 3]
            self.filter_reply(self.mock_a, prompt, keep)
            self.mock_b.scenarios[prompt] = [Reply(body=completion(category))]
            if keep:
                accepted.append(prompt)
                counts[category] += 1
        suffix = ("WHERE AI_FILTER('semantic_a', prompt) GROUP BY category "
                  "HAVING n >= 2 ORDER BY n DESC, category LIMIT 2")
        fields = "AI_MAP('semantic_b', prompt) AS category, COUNT(*) AS n"
        self.assert_plan_labels(self.statement(fields, suffix),
                                ("SEMANTIC MAP", "SEMANTIC FILTER", "GROUP"))
        expected = tuple(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:2])
        self.assertEqual(self.select(fields, suffix), expected)
        self.assert_counts(self.mock_a, prompts)
        self.assert_counts(self.mock_b, accepted)

    def test_numeric_map_aggregate_parameters(self):
        prompts = [f"score-{index}" for index in range(41)]
        self.load(prompts)
        sums = [Decimal(0) for _ in range(3)]
        for index, prompt in enumerate(prompts):
            score = Decimal(index) + Decimal("0.25")
            sums[index % 3] += score
            self.mock_a.scenarios[prompt] = [Reply(body=completion(str(score)))]
        fields = "id % 3 AS g, SUM(CAST(AI_MAP('semantic_a', prompt) AS DECIMAL(12, 2))) AS score"
        self.assertEqual(self.select(fields, "GROUP BY g ORDER BY g"), tuple(enumerate(sums)))
        self.assert_counts(self.mock_a, prompts)

    def test_inner_join_pair_filter_and_map(self):
        prompts = [f"pair-{index}" for index in range(9)]
        self.load(prompts)
        pairs = []
        accepted = []
        expected = []
        for left in range(len(prompts)):
            for right in range(left + 1, len(prompts)):
                pair = f"{prompts[left]}|{prompts[right]}"
                pairs.append(pair)
                keep = (left + right) % 3 == 0
                self.filter_reply(self.mock_a, pair, keep)
                self.mock_b.scenarios[pair] = [Reply(body=completion(f"{left}:{right}"))]
                if keep:
                    accepted.append(pair)
                    expected.append((left, right, f"{left}:{right}"))
        pair_expr = "CONCAT(a.prompt, '|', b.prompt)"
        fields = f"a.id, b.id, AI_MAP('semantic_b', {pair_expr})"
        suffix = ("a JOIN semantic_inputs b ON a.id < b.id "
                  f"WHERE AI_FILTER('semantic_a', {pair_expr}) ORDER BY a.id, b.id")
        self.assertEqual(self.select(fields, suffix), tuple(expected))
        self.assert_counts(self.mock_a, pairs)
        self.assert_counts(self.mock_b, accepted)

    def test_semantic_having_runs_on_groups_not_input_rows(self):
        prompts = [f"having-{index}" for index in range(40)]
        self.load(prompts)
        self.filter_reply(self.mock_a, "g:0:20", False)
        self.filter_reply(self.mock_a, "g:1:20", True)
        fields = "id % 2 AS g, COUNT(*) AS n"
        suffix = ("GROUP BY g HAVING AI_FILTER('semantic_a', "
                  "CONCAT('g:', g, ':', COUNT(*))) ORDER BY g")
        self.assertEqual(self.select(fields, suffix), ((1, 20),))
        self.assert_counts(self.mock_a, ["g:0:20", "g:1:20"])
        self.assertFalse(self.mock_b.requests)

    def test_filter_ordered_limit_offset(self):
        prompts = [f"offset-{index}" for index in range(61)]
        self.load(prompts)
        kept = []
        for index, prompt in enumerate(prompts):
            keep = index % 4 != 1
            self.filter_reply(self.mock_a, prompt, keep)
            if keep:
                kept.append((index,))
        suffix = ("WHERE AI_FILTER('semantic_a', prompt) "
                  "ORDER BY id LIMIT 7 OFFSET 3")
        self.assertEqual(self.select("id", suffix), tuple(kept[3:10]))
        self.assertFalse(self.mock_b.requests)
        self.assertTrue(self.mock_a.requests)
        self.assertTrue(all(count == 1 for count in self.mock_a.counts.values()))

    def test_noncorrelated_derived_semantic_stages(self):
        prompts = [f"derived-{index}" for index in range(35)]
        self.load(prompts)
        counts = Counter()
        accepted = []
        for index, prompt in enumerate(prompts):
            keep = index % 5 != 0
            category = f"class-{index % 3}"
            self.filter_reply(self.mock_a, prompt, keep)
            self.mock_b.scenarios[prompt] = [Reply(body=completion(category))]
            if keep:
                accepted.append(prompt)
                counts[category] += 1
        inner = self.statement("AI_MAP('semantic_b', prompt) AS category",
                               "WHERE AI_FILTER('semantic_a', prompt)")
        self.cursor.execute("SELECT category, COUNT(*) FROM (" + inner +
                            ") classified GROUP BY category ORDER BY category")
        self.assertEqual(self.cursor.fetchall(), tuple(sorted(counts.items())))
        self.assert_counts(self.mock_a, prompts)
        self.assert_counts(self.mock_b, accepted)

    def test_three_valued_boolean_logic(self):
        prompts = ["true-result", "false-result"]
        self.load(prompts)
        self.filter_reply(self.mock_a, prompts[0], True)
        self.filter_reply(self.mock_a, prompts[1], False)
        fields = ("id, NULL OR AI_FILTER('semantic_a', prompt), "
                  "NULL AND AI_FILTER('semantic_a', prompt)")
        self.assertEqual(self.select(fields), ((0, 1, None), (1, None, 0)))
        self.assert_counts(self.mock_a, prompts * 2)

    def test_constant_prompts_remain_per_row(self):
        self.load([f"constant-{index}" for index in range(37)])
        self.mock_a.scenarios["constant-input"] = [Reply(body=completion("same-text"))]
        rows = self.select("id, AI_MAP('semantic_a', 'constant-input')")
        self.assertEqual(rows, tuple((index, "same-text") for index in range(37)))
        self.assert_counts(self.mock_a, ["constant-input"] * 37)

    def test_gated_nonprefix_sort_retains_collection_across_waits(self):
        prompts = [f"sort-source-{index}" for index in range(67)]
        self.load(prompts)
        gate = self.fixtures.gate()
        for index, prompt in enumerate(prompts):
            self.mock_a.scenarios[prompt] = [
                Reply(body=completion(f"key-{66 - index:03d}"), gate=gate)]
        fields = "id, AI_MAP('semantic_a', prompt) AS sort_key"
        suffix = "ORDER BY sort_key, id"
        self.assert_plan_labels(self.statement(fields, suffix), ("SEMANTIC MAP", "SORT"))
        pending = self.fixtures.start(lambda: self.select(fields, suffix))
        semantic.wait_evidence(self, self.mock_a,
                               lambda: len(self.mock_a.requests) == 2 * semantic.BATCH_ROWS,
                               pending, message="sort must collect another semantic input batch")
        semantic.assert_held(self, self.mock_a, [gate])
        gate.set()
        self.assertEqual(pending.result(),
                         tuple((index, f"key-{66 - index:03d}") for index in reversed(range(67))))
        self.assert_counts(self.mock_a, prompts)

    def test_gated_hash_groupby_retains_collection_across_waits(self):
        prompts = [f"hash-source-{index}" for index in range(67)]
        self.load(prompts)
        gate = self.fixtures.gate()
        counts = Counter(str(index % 5) for index in range(67))
        for index, prompt in enumerate(prompts):
            self.mock_a.scenarios[prompt] = [
                Reply(body=completion(str(index % 5)), gate=gate)]
        statement = self.statement("AI_MAP('semantic_a', prompt) AS category, COUNT(*)",
                                   "GROUP BY category").replace(
                                       "SELECT /*+ ", "SELECT /*+ USE_HASH_AGGREGATION ")
        self.assert_plan_labels(statement, ("SEMANTIC MAP", "HASH GROUP BY"))

        def execute():
            self.cursor.execute(statement)
            return self.cursor.fetchall()

        pending = self.fixtures.start(execute)
        semantic.wait_evidence(self, self.mock_a,
                               lambda: len(self.mock_a.requests) == 2 * semantic.BATCH_ROWS,
                               pending, message="aggregation must collect another semantic input batch")
        semantic.assert_held(self, self.mock_a, [gate])
        gate.set()
        self.assertEqual(sorted(pending.result()), sorted(counts.items()))
        self.assert_counts(self.mock_a, prompts)

    def test_having_shortcircuit_materializes_projected_boolean_without_replay(self):
        prompts = ["having-true", "having-skipped-false", "having-rejected"]
        self.load(prompts)
        for prompt, value in zip(prompts, (True, False, False)):
            self.filter_reply(self.mock_a, prompt, value)
        fields = "id, AI_FILTER('semantic_a', prompt) AS b"
        suffix = "HAVING CASE WHEN id = 1 THEN TRUE ELSE b END ORDER BY id"
        self.assertEqual(self.select(fields, suffix), ((0, 1), (1, 0)))
        self.assert_counts(self.mock_a, prompts)

    def test_streaming_having_completes_partial_projection_without_pinning_credits(self):
        prompts = [f"streaming-having-{index}" for index in range(67)]
        self.load(prompts)
        for index, prompt in enumerate(prompts):
            self.filter_reply(self.mock_a, prompt, index % 3 != 0)
        fields = "id, AI_FILTER('semantic_a', prompt) AS b"
        suffix = "HAVING CASE WHEN MOD(id, 7) = 0 THEN TRUE ELSE b END"
        self.assertEqual(sorted(self.select(fields, suffix)),
                         [(index, int(index % 3 != 0)) for index in range(67)
                          if index % 7 == 0 or index % 3 != 0])
        self.assert_counts(self.mock_a, prompts)

    def test_having_shared_nested_map_recomputes_missing_prompt_intermediates(self):
        prompts = ["nested-shared-0", "nested-shared-1", "nested-shared-2"]
        self.load(prompts)
        for index, prompt in enumerate(prompts):
            self.mock_a.scenarios[prompt] = [Reply(body=completion(f"inner-{index}"))]
            self.mock_b.scenarios[f"outer:inner-{index}"] = [
                Reply(body=completion(f"outer-{index}"))]
        fields = ("id, AI_MAP('semantic_b', "
                  "CONCAT('outer:', AI_MAP('semantic_a', prompt))) AS b")
        suffix = "HAVING CASE WHEN id = 1 THEN TRUE ELSE LENGTH(b) > 0 END ORDER BY id"
        self.assertEqual(self.select(fields, suffix),
                         tuple((index, f"outer-{index}") for index in range(3)))
        self.assert_counts(self.mock_a, prompts)
        self.assert_counts(self.mock_b, [f"outer:inner-{index}" for index in range(3)])

    def test_shared_prompt_alias_materializes_after_having_without_null_fallback(self):
        prompts = ["prompt-alias-0", "prompt-alias-1", "prompt-alias-2"]
        self.load(prompts)
        for index, prompt in enumerate(prompts):
            self.mock_a.scenarios[prompt] = [Reply(body=completion(f"inner-{index}"))]
            self.filter_reply(self.mock_b, f"outer:inner-{index}", True)
        fields = ("id, CONCAT('outer:', AI_MAP('semantic_a', prompt)) AS p")
        suffix = ("HAVING CASE WHEN id = 1 THEN TRUE "
                  "ELSE AI_FILTER('semantic_b', p) END ORDER BY id")
        self.assertEqual(self.select(fields, suffix),
                         tuple((index, f"outer:inner-{index}") for index in range(3)))
        self.assert_counts(self.mock_a, prompts)
        self.assert_counts(self.mock_b, ["outer:inner-0", "outer:inner-2"])

    def test_nested_prompt_cpu_fallback_is_not_evaluated_against_a_missing_tag(self):
        prompts = ["not-json-0", "not-json-1", "not-json-2"]
        self.load(prompts)
        for index, prompt in enumerate(prompts):
            self.mock_a.scenarios[prompt] = [Reply(body=completion(f"inner-{index}"))]
            self.filter_reply(self.mock_b, f"outer:inner-{index}", True)
        fields = ("id, CONCAT('outer:', IFNULL(AI_MAP('semantic_a', prompt), "
                  "JSON_UNQUOTE(JSON_EXTRACT(prompt, '$')))) AS p")
        suffix = ("HAVING CASE WHEN id = 1 THEN TRUE "
                  "ELSE AI_FILTER('semantic_b', p) END ORDER BY id")
        self.assertEqual(self.select(fields, suffix),
                         tuple((index, f"outer:inner-{index}") for index in range(3)))
        self.assert_counts(self.mock_a, prompts)
        self.assert_counts(self.mock_b, ["outer:inner-0", "outer:inner-2"])

    def test_having_complete_boolean_target_skips_invalid_cpu_or_branch(self):
        prompts = ["boolean-not-json-0", "boolean-not-json-1", "boolean-not-json-2"]
        self.load(prompts)
        for prompt in prompts:
            self.filter_reply(self.mock_a, prompt, True)
        fields = ("id, (AI_FILTER('semantic_a', prompt) OR "
                  "(JSON_EXTRACT(prompt, '$') IS NOT NULL)) AS b")
        self.assertEqual(self.select(fields, "HAVING b ORDER BY id"),
                         ((0, 1), (1, 1), (2, 1)))
        self.assert_counts(self.mock_a, prompts)

    def test_having_cpu_case_guard_remains_valid_for_an_ordinary_sort_consumer(self):
        prompts = [f"cpu-guard-sort-{index}" for index in range(67)]
        self.load(prompts)
        for index, prompt in enumerate(prompts):
            self.filter_reply(self.mock_a, prompt, index % 3 != 0)
        fields = "id, AI_FILTER('semantic_a', prompt) AS b"
        suffix = ("HAVING CASE WHEN MOD(id, 7) = 0 THEN TRUE ELSE b END "
                  "ORDER BY MOD(id, 7), id")
        expected = [(index, int(index % 3 != 0)) for index in range(67)
                    if index % 7 == 0 or index % 3 != 0]
        self.assertEqual(self.select(fields, suffix),
                         tuple(sorted(expected, key=lambda row: (row[0] % 7, row[0]))))
        self.assert_counts(self.mock_a, prompts)

    def test_having_projection_preserves_its_own_case_guard(self):
        prompts = [None, "projected-valid", "projected-rejected"]
        self.load(prompts)
        self.filter_reply(self.mock_a, prompts[1], True)
        self.filter_reply(self.mock_a, prompts[2], False)
        fields = "id, CASE WHEN id = 0 THEN FALSE ELSE AI_FILTER('semantic_a', prompt) END AS b"
        suffix = "HAVING CASE WHEN id = 0 THEN TRUE ELSE b END ORDER BY id"
        self.assertEqual(self.select(fields, suffix), ((0, 0), (1, 1)))
        self.assert_counts(self.mock_a, prompts[1:])

    def test_grouped_having_map_projection_and_sort_materialize_once_per_group(self):
        self.load([f"grouped-input-{index}" for index in range(67)])
        for group, value in enumerate(("keep", "drop", "drop")):
            self.mock_a.scenarios[f"group-{group}"] = [Reply(body=completion(value))]
        fields = ("MOD(id, 3) AS g, AI_MAP('semantic_a', CONCAT('group-', MOD(id, 3))) AS label, "
                  "COUNT(*) AS n")
        suffix = ("GROUP BY g HAVING CASE WHEN g = 1 THEN TRUE ELSE label = 'keep' END "
                  "ORDER BY label, g")
        self.assertEqual(self.select(fields, suffix), ((1, "drop", 22), (0, "keep", 23)))
        self.assert_counts(self.mock_a, ["group-0", "group-1", "group-2"])

    def test_having_projection_materializes_only_rows_after_limit(self):
        self.load(["selected-after-limit", None, None])
        self.filter_reply(self.mock_a, "selected-after-limit", False)
        fields = "id, AI_FILTER('semantic_a', prompt) AS b"
        suffix = "HAVING CASE WHEN id >= 0 THEN TRUE ELSE b END ORDER BY id LIMIT 1"
        self.assertEqual(self.select(fields, suffix), ((0, 0),))
        self.assert_counts(self.mock_a, ["selected-after-limit"])

    def test_having_projection_does_not_prepare_discarded_cpu_prompt_arguments(self):
        self.load(['{"value":"selected-cpu-prompt"}', "not-json", "also-not-json"])
        self.mock_a.scenarios["selected-cpu-prompt"] = [Reply(body=completion("selected-result"))]
        fields = ("id, AI_MAP('semantic_a', "
                  "JSON_UNQUOTE(JSON_EXTRACT(prompt, '$.value'))) AS b")
        suffix = "HAVING CASE WHEN id >= 0 THEN TRUE ELSE LENGTH(b) > 0 END ORDER BY id LIMIT 1"
        self.assertEqual(self.select(fields, suffix), ((0, "selected-result"),))
        self.assert_counts(self.mock_a, ["selected-cpu-prompt"])

    def test_conditional_sort_carries_skipped_projection_past_limit(self):
        self.load(["sort-carried-true", None, "sort-carried-false"])
        self.filter_reply(self.mock_a, "sort-carried-true", True)
        self.filter_reply(self.mock_a, "sort-carried-false", False)
        fields = "id, AI_FILTER('semantic_a', prompt) AS b"
        suffix = "ORDER BY CASE WHEN id = 1 THEN TRUE ELSE b END, id LIMIT 1"
        self.assertEqual(self.select(fields, suffix), ((2, 0),))
        self.assert_counts(self.mock_a, ["sort-carried-true", "sort-carried-false"])

    def test_conditional_sort_materializes_shared_projection_once(self):
        prompts = ["sort-shared-true", "sort-shared-projection", "sort-shared-false"]
        self.load(prompts)
        self.filter_reply(self.mock_a, prompts[0], True)
        self.filter_reply(self.mock_a, prompts[1], False)
        self.filter_reply(self.mock_a, prompts[2], False)
        fields = "id, AI_FILTER('semantic_a', prompt) AS b"
        suffix = "ORDER BY CASE WHEN id = 1 THEN TRUE ELSE b END, id"
        self.assertEqual(self.select(fields, suffix), ((2, 0), (0, 1), (1, 0)))
        self.assert_counts(self.mock_a, prompts)

    def test_conditional_map_sort_carries_partial_lob_results_across_batches(self):
        prompts = [f"partial-lob-{index}" for index in range(67)]
        self.load(prompts)
        labels = [f"label-{66 - index:03d}" for index in range(67)]
        for prompt, label in zip(prompts, labels):
            self.mock_a.scenarios[prompt] = [Reply(body=completion(label))]
        fields = "id, AI_MAP('semantic_a', prompt) AS label"
        suffix = "ORDER BY CASE WHEN MOD(id, 3) = 1 THEN 'zz' ELSE label END, id"
        indices = sorted(range(67),
                         key=lambda index: ("zz" if index % 3 == 1 else labels[index], index))
        self.assertEqual(self.select(fields, suffix),
                         tuple((index, labels[index]) for index in indices))
        self.assert_counts(self.mock_a, prompts)

    def test_empty_map_result_remains_ready_when_a_peer_needs_materialization(self):
        prompts = ["shared-empty", "shared-missing", "shared-nonempty"]
        self.load(prompts)
        labels = ("", "a", "b")
        for prompt, label in zip(prompts, labels):
            self.mock_a.scenarios[prompt] = [Reply(body=completion(label))]
        fields = "id, AI_MAP('semantic_a', prompt) AS label"
        suffix = "ORDER BY CASE WHEN id = 1 THEN 'a' ELSE label END, id"
        self.assertEqual(self.select(fields, suffix), tuple(enumerate(labels)))
        self.assert_counts(self.mock_a, prompts)

    def test_pinned_filter_batch_memory_fails_with_quota_not_deadline(self):
        payload = b"x" * 40000
        self.cursor.execute("CREATE TABLE semantic_memory_inputs "
                            "(id INT PRIMARY KEY, payload VARBINARY(50000))")
        self.cursor.executemany("INSERT INTO semantic_memory_inputs VALUES (%s, %s)",
                                [(index, payload) for index in range(16)])
        self.filter_reply(self.mock_a, "keep", True)
        statement = ("SELECT /*+ OPT_PARAM('rowsets_max_rows', 16) */ "
                     "id, payload, AI_MAP('semantic_b', 'map') FROM semantic_memory_inputs "
                     "WHERE AI_FILTER('semantic_a', 'keep') ORDER BY id")
        self.cursor.execute("SET ai_pipeline_slots=1, ai_pipeline_memory_limit=1048576, "
                            "ob_query_timeout=3000000")
        semantic.sql_error(self, 4019, lambda: self.cursor.execute(statement))
        self.assert_counts(self.mock_a, ["keep"] * 16)
        self.assertFalse(self.mock_b.requests)
        self.cursor.execute("SET ai_pipeline_memory_limit=DEFAULT, ob_query_timeout=20000000")
        self.cursor.execute(statement)
        self.assertEqual(self.cursor.fetchall(),
                         tuple((index, payload, semantic.answer("map")) for index in range(16)))
        self.assert_counts(self.mock_a, ["keep"] * 32)
        self.assert_counts(self.mock_b, ["map"] * 16)


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    if "--case" not in arguments and "--self-test" not in arguments:
        for name in sorted(SemanticCompositionContracts.__dict__):
            if name.startswith("test_"):
                arguments.extend(["--case", name])
    semantic.SemanticContracts = SemanticCompositionContracts
    semantic.main(arguments)


if __name__ == "__main__":
    main()
