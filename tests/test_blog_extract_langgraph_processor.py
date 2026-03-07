import importlib
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_PATH = PROJECT_ROOT / "dags"
if str(DAGS_PATH) not in sys.path:
    sys.path.insert(0, str(DAGS_PATH))


def install_dependency_stubs():
    airflow_module = types.ModuleType("airflow")
    airflow_models = types.ModuleType("airflow.models")

    class VariableStub:
        @staticmethod
        def get(_name, default_var=None):
            return default_var

    airflow_models.Variable = VariableStub
    airflow_module.models = airflow_models
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.models"] = airflow_models

    postgres_hook_module = types.ModuleType("airflow.providers.postgres.hooks.postgres")

    class PostgresHookStub:
        def __init__(self, *args, **kwargs):
            pass

        def get_conn(self):
            raise RuntimeError("db access is not available in unit tests")

    postgres_hook_module.PostgresHook = PostgresHookStub
    sys.modules["airflow.providers"] = types.ModuleType("airflow.providers")
    sys.modules["airflow.providers.postgres"] = types.ModuleType("airflow.providers.postgres")
    sys.modules["airflow.providers.postgres.hooks"] = types.ModuleType("airflow.providers.postgres.hooks")
    sys.modules["airflow.providers.postgres.hooks.postgres"] = postgres_hook_module

    output_parser_module = types.ModuleType("langchain_core.output_parsers")

    class PydanticOutputParserStub:
        def __init__(self, *args, **kwargs):
            pass

        def get_format_instructions(self):
            return "JSON"

        def __ror__(self, other):
            return self

    output_parser_module.PydanticOutputParser = PydanticOutputParserStub
    sys.modules["langchain_core.output_parsers"] = output_parser_module

    prompts_module = types.ModuleType("langchain_core.prompts")

    class PromptChainStub:
        def __or__(self, other):
            return self

        def partial(self, **_kwargs):
            return self

        def invoke(self, _payload):
            return None

    class ChatPromptTemplateStub:
        @staticmethod
        def from_messages(_messages):
            return PromptChainStub()

    prompts_module.ChatPromptTemplate = ChatPromptTemplateStub
    sys.modules["langchain_core.prompts"] = prompts_module

    google_module = types.ModuleType("langchain_google_genai")

    class ChatGoogleGenerativeAIStub:
        def __init__(self, *args, **kwargs):
            pass

    google_module.ChatGoogleGenerativeAI = ChatGoogleGenerativeAIStub
    sys.modules["langchain_google_genai"] = google_module

    langgraph_module = types.ModuleType("langgraph.graph")

    class CompiledGraphStub:
        def invoke(self, _inputs):
            return {"results": []}

    class StateGraphStub:
        def __init__(self, *_args, **_kwargs):
            pass

        def add_node(self, *_args, **_kwargs):
            pass

        def set_entry_point(self, *_args, **_kwargs):
            pass

        def add_edge(self, *_args, **_kwargs):
            pass

        def compile(self):
            return CompiledGraphStub()

    langgraph_module.END = object()
    langgraph_module.StateGraph = StateGraphStub
    sys.modules["langgraph.graph"] = langgraph_module


class BlogExtractLanggraphProcessorImportTest(unittest.TestCase):
    def tearDown(self):
        sys.modules.pop("logics.blog_extract_langgraph_processor", None)

    def test_import_does_not_require_google_api_key(self):
        install_dependency_stubs()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOOGLE_API_KEY", None)
            module = importlib.import_module("logics.blog_extract_langgraph_processor")

        self.assertTrue(hasattr(module, "main_processor"))


class BlogExtractLanggraphProcessorSignalPostProcessingTest(unittest.TestCase):
    def tearDown(self):
        sys.modules.pop("logics.blog_extract_langgraph_processor", None)

    def load_module(self):
        install_dependency_stubs()
        return importlib.import_module("logics.blog_extract_langgraph_processor")

    def test_build_prompt_includes_stricter_extraction_rules(self):
        module = self.load_module()

        prompt = module.build_core_ideas_system_prompt()

        self.assertIn("명시적으로 주장", prompt)
        self.assertIn("근거 문장", prompt)
        self.assertIn("모호하면 추측하지 말고 제외", prompt)
        self.assertIn("최대 3개", prompt)

    def test_post_process_signals_filters_invalid_duplicates_and_limits_results(self):
        module = self.load_module()
        cleaned_text = (
            "OXY를 더 사도 된다고 본다. "
            "GS는 관망 의견이다. "
            "한전KPS는 매수 의견이다. "
            "AMZN은 지금은 기다려야 한다."
        )
        raw_signals = [
            {
                "ticker": "OXY",
                "action": "buy",
                "evidence": "OXY를 더 사도 된다고 본다.",
                "confidence_score": 0.95,
            },
            {
                "ticker": "OXY",
                "action": "hold",
                "evidence": "OXY를 더 사도 된다고 본다.",
                "confidence_score": 0.60,
            },
            {
                "ticker": "GS",
                "action": "hold",
                "evidence": "GS는 관망 의견이다.",
                "confidence_score": 0.80,
            },
            {
                "ticker": "한전KPS",
                "action": "buy",
                "evidence": "한전KPS는 매수 의견이다.",
                "confidence_score": 0.85,
            },
            {
                "ticker": "AMZN",
                "action": "hold",
                "evidence": "AMZN은 지금은 기다려야 한다.",
                "confidence_score": 0.70,
            },
            {
                "ticker": "",
                "action": "buy",
                "evidence": "GS는 관망 의견이다.",
                "confidence_score": 0.90,
            },
            {
                "ticker": "NVDA",
                "action": "buy",
                "evidence": "본문에 없는 근거 문장",
                "confidence_score": 0.99,
            },
        ]

        processed = module.post_process_signals(cleaned_text, raw_signals)

        self.assertEqual(
            processed,
            [
                {
                    "ticker": "OXY",
                    "action": "buy",
                    "evidence": "OXY를 더 사도 된다고 본다.",
                    "confidence_score": 0.95,
                },
                {
                    "ticker": "한전KPS",
                    "action": "buy",
                    "evidence": "한전KPS는 매수 의견이다.",
                    "confidence_score": 0.85,
                },
                {
                    "ticker": "GS",
                    "action": "hold",
                    "evidence": "GS는 관망 의견이다.",
                    "confidence_score": 0.80,
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
