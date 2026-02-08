# Teste de integração leve (não faz requests). Apenas garante imports/estrutura.
from ufc_pipeline.orchestration import runner

def test_imports():
    assert callable(runner.run_stage)
