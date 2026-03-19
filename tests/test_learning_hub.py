from __future__ import annotations

import unittest

from quant_exchange.learning import LearningHubService


class LearningHubTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = LearningHubService()

    def test_lh_01_public_hub_hides_quiz_answers(self) -> None:
        payload = self.service.hub_payload()
        self.assertIn("overview", payload)
        self.assertGreaterEqual(len(payload["knowledge_base"]), 10)
        self.assertGreaterEqual(len(payload["lessons"]), 10)
        self.assertGreaterEqual(len(payload["quiz"]["questions"]), 10)
        categories = {section["category"] for section in payload["knowledge_base"]}
        self.assertIn("宏观经济与财经周期", categories)
        self.assertIn("货币、利率与汇率", categories)
        self.assertIn("股票、行业与公司分析", categories)
        self.assertIn("期货、期权与衍生品", categories)
        self.assertIn("区块链与数字资产", categories)
        self.assertIn("银行体系与信用创造", categories)
        self.assertIn("保险与保障体系", categories)
        total_entries = sum(len(section["entries"]) for section in payload["knowledge_base"])
        self.assertGreaterEqual(total_entries, 70)
        self.assertNotIn("correct_option_id", payload["quiz"]["questions"][0])
        self.assertNotIn("explanation", payload["quiz"]["questions"][0])

    def test_lh_02_quiz_scoring_returns_review_guidance(self) -> None:
        result = self.service.evaluate_quiz(
            {
                "q1": "a",
                "q2": "c",
                "q3": "a",
                "q4": "d",
                "q5": "c",
                "q6": "d",
                "q7": "a",
                "q8": "d",
            }
        )
        self.assertLess(result["score"], result["pass_score"])
        self.assertFalse(result["passed"])
        self.assertGreaterEqual(len(result["weak_lessons"]), 1)
        self.assertGreaterEqual(len(result["recommended_next_steps"]), 2)
        incorrect = [item for item in result["results"] if not item["is_correct"]]
        self.assertGreaterEqual(len(incorrect), 1)
        self.assertTrue(incorrect[0]["explanation"])

    def test_lh_03_full_score_returns_passed_result(self) -> None:
        answers = {
            question["question_id"]: question["correct_option_id"]
            for question in self.service._hub["quiz"]["questions"]
        }
        result = self.service.evaluate_quiz(answers)
        self.assertEqual(result["score"], 100)
        self.assertTrue(result["passed"])
        self.assertEqual(result["correct_count"], result["total_questions"])
        self.assertEqual(result["weak_lessons"], [])


if __name__ == "__main__":
    unittest.main()
